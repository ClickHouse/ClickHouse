#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: clickhouse-proxy is built on the silk fiber framework, which the fast build omits.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# When a rule matches on the user, the proxy terminates the MySQL handshake to read the user name.
# MySQL negotiates TLS in-band, so the proxy has to terminate it as well: with a server certificate,
# its greeting offers TLS, and a client that asks for it sends the user name and the authentication
# response over TLS (the backend leg stays in plaintext). A client that does not ask for TLS is served
# in plaintext on the same listener.
# The greeting is sent before the backend is known, so its connection ID cannot be the backend's:
# the IDs are unique among the sessions of the proxy and are taken from the upper half of the range,
# apart from the IDs of a server, which counts its connections from zero.

DIR="${CLICKHOUSE_TMP}/05896_proxy"
rm -rf "$DIR"
mkdir -p "$DIR"
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$DIR/server.key" -out "$DIR/server.crt" -days 1 -subj /CN=localhost 2>/dev/null

python3 - "$CLICKHOUSE_BINARY" "$DIR" <<'EOF'
import socket, ssl, struct, subprocess, sys, threading, time

binary, tmp = sys.argv[1], sys.argv[2]

CLIENT_CONNECT_WITH_DB = 0x8
CLIENT_PROTOCOL_41 = 0x200
CLIENT_SSL = 0x800
CLIENT_SECURE_CONNECTION = 0x8000
CLIENT_PLUGIN_AUTH = 0x80000
CAPABILITIES = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH

def packet(sequence_id, payload):
    return struct.pack('<I', len(payload))[:3] + bytes([sequence_id]) + payload

def read_exactly(conn, size):
    data = b''
    while len(data) < size:
        chunk = conn.recv(size - len(data))
        if not chunk:
            raise EOFError
        data += chunk
    return data

def read_packet(conn):
    header = read_exactly(conn, 4)
    return header[3], read_exactly(conn, header[0] | (header[1] << 8) | (header[2] << 16))

# A backend that accepts any login and answers every command with its own payload.
# It tells what the login looked like: on the backend leg there is no TLS.
logins = []
logins_done = threading.Condition()

def serve_connection(conn):
    try:
        scramble = b'0123456789abcdefghij'
        greeting = (b'\x0a' + b'8.0.0-fake\0' + struct.pack('<I', 7) + scramble[:8] + b'\0'
            + struct.pack('<H', (CAPABILITIES | CLIENT_SSL) & 0xffff) + b'\x21' + struct.pack('<H', 2)
            + struct.pack('<H', CAPABILITIES >> 16) + bytes([21]) + b'\0' * 10
            + scramble[8:] + b'\0' + b'mysql_native_password\0')
        conn.sendall(packet(0, greeting))
        sequence_id, login = read_packet(conn)
        capabilities = struct.unpack('<I', login[:4])[0]
        user = login[32:login.index(b'\0', 32)].decode()
        with logins_done:
            logins.append(f'user {user}, asks for TLS: {bool(capabilities & CLIENT_SSL)}')
            logins_done.notify_all()
        conn.sendall(packet(sequence_id + 1, b'\x00\x00\x00\x02\x00\x00\x00'))
        while True:
            sequence_id, command = read_packet(conn)
            conn.sendall(packet(sequence_id + 1, b'echo: ' + command[1:]))
    except (EOFError, OSError):
        pass
    conn.close()

def serve(server):
    while True:
        conn, _ = server.accept()
        conn.settimeout(60)
        threading.Thread(target=serve_connection, args=(conn,), daemon=True).start()

server = socket.socket()
server.bind(('127.0.0.1', 0))
server.listen(16)
backend_port = server.getsockname()[1]
threading.Thread(target=serve, args=(server,), daemon=True).start()

def start_proxy():
    # A free port is picked by binding a socket and releasing it, so another process can take it
    # before the proxy binds it: the caller retries on a fresh port.
    probe = socket.socket()
    probe.bind(('127.0.0.1', 0))
    port = probe.getsockname()[1]
    probe.close()
    config = f'{tmp}/proxy_config.xml'
    with open(config, 'w') as f:
        f.write(f'''<clickhouse>
    <logger><level>information</level><console>1</console></logger>
    <proxy>
        <listen_host>127.0.0.1</listen_host>
        <listeners>
            <listener><protocol>mysql</protocol><port>{port}</port></listener>
        </listeners>
        <pools>
            <pool>
                <name>my</name>
                <backend><host>127.0.0.1</host><mysql_port>{backend_port}</mysql_port></backend>
            </pool>
        </pools>
        <rules>
            <rule><user>user_05896</user><pool>my</pool></rule>
        </rules>
        <health_check><enabled>0</enabled></health_check>
    </proxy>
    <openSSL>
        <server>
            <certificateFile>{tmp}/server.crt</certificateFile>
            <privateKeyFile>{tmp}/server.key</privateKeyFile>
        </server>
    </openSSL>
</clickhouse>
''')
    log = f'{tmp}/proxy.log'
    proc = subprocess.Popen([binary, 'proxy', '--config-file', config],
        stdout=open(log, 'w'), stderr=subprocess.STDOUT)
    # Wait for our own proxy to report its listener rather than probing the port: a probe can be
    # answered by another process that took the port, while our proxy is still starting and fails to bind.
    listening = f'Listening for mysql on 127.0.0.1:{port}'
    for _ in range(300):
        if proc.poll() is not None:
            return None, None
        with open(log, errors='replace') as f:
            if listening in f.read():
                return proc, port
        time.sleep(0.1)
    proc.kill()
    proc.wait()
    return None, None

proc, port = None, None
for _ in range(5):
    proc, port = start_proxy()
    if proc:
        break
if not proc:
    print('proxy did not start')
    sys.exit(0)

connection_ids = []

def session(with_tls):
    print('client with TLS:' if with_tls else 'client without TLS:')
    conn = socket.create_connection(('127.0.0.1', port), timeout=60)
    sequence_id, greeting = read_packet(conn)
    version_end = greeting.index(b'\0', 1)
    connection_ids.append(struct.unpack('<I', greeting[version_end + 1:version_end + 5])[0])
    offered = struct.unpack('<H', greeting[version_end + 14:version_end + 16])[0]
    print('  the greeting offers TLS:', bool(offered & CLIENT_SSL))

    capabilities = CAPABILITIES | (CLIENT_SSL if with_tls else 0)
    head = struct.pack('<IIB', capabilities, 1 << 24, 33) + b'\0' * 23
    if with_tls:
        # The `SSLRequest` is the head of a handshake response; the response itself follows over TLS.
        sequence_id += 1
        conn.sendall(packet(sequence_id, head))
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        conn = context.wrap_socket(conn)
        print('  TLS is established:', conn.version() is not None)
    sequence_id += 1
    conn.sendall(packet(sequence_id, head + b'user_05896\0' + bytes([20]) + b'x' * 20 + b'mysql_native_password\0'))

    sequence_id, payload = read_packet(conn)
    print('  auth-switch request:', payload[:1] == b'\xfe')
    conn.sendall(packet(sequence_id + 1, b'y' * 20))
    sequence_id, payload = read_packet(conn)
    print('  logged in:', payload[:1] == b'\x00')
    with logins_done:
        logins_done.wait_for(lambda: logins, timeout=60)
        print('  login on the backend leg:', logins.pop() if logins else 'none')

    # The command phase is relayed in both directions.
    for query in (b'SELECT 1', b'SELECT ' + b'2' * 100000):
        conn.sendall(packet(0, b'\x03' + query))
        _, payload = read_packet(conn)
        print('  answer:', payload == b'echo: ' + query)
    conn.close()

try:
    session(with_tls=True)
    session(with_tls=False)
    print('connection IDs are unique:', len(set(connection_ids)) == len(connection_ids))
    print('connection IDs are apart from the IDs of a server:', all(i >= 0x80000000 for i in connection_ids))
finally:
    proc.kill()
    proc.wait()
EOF
