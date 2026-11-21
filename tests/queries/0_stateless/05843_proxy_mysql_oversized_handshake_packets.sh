#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: clickhouse-proxy is built on the silk fiber framework, which the fast build omits.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# When a rule matches on the user, the proxy terminates the MySQL handshake and reads packets of both legs
# before the backend has authenticated the client. Neither peer is trusted at that point, so every packet
# of the authentication phase is bounded: a packet that declares a larger payload (which could continue
# over further packets without a limit) ends the connection instead of being read into memory.
# Here a fake backend sends an oversized greeting, and a client sends an oversized auth-switch response:
# each streams up to 64 MiB, and the proxy must drop the connection long before that.

python3 - "$CLICKHOUSE_BINARY" "$CLICKHOUSE_TMP" <<'EOF'
import socket, struct, subprocess, sys, threading, time

binary, tmp = sys.argv[1], sys.argv[2]

CLIENT_PROTOCOL_41 = 0x200
CLIENT_SECURE_CONNECTION = 0x8000
CLIENT_PLUGIN_AUTH = 0x80000
CAPABILITIES = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH

STREAM_LIMIT = 64 * 1024 * 1024
# Far above what the socket buffers of both sides can hold, and far below the stream.
REJECTED_BELOW = 16 * 1024 * 1024

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

def stream_oversized(conn, sequence_id, first_byte):
    """Declares a payload of the maximum size and streams it. Returns the number of bytes the peer accepted."""
    sent = 0
    try:
        conn.sendall(b'\xff\xff\xff' + bytes([sequence_id]) + first_byte)
        block = b'A' * 65536
        while sent < STREAM_LIMIT:
            conn.sendall(block)
            sent += len(block)
    except OSError:
        pass
    return sent

mode = 'oversized greeting'
backend_results = []
backend_done = threading.Condition()

def serve(server):
    while True:
        conn, _ = server.accept()
        conn.settimeout(60)
        result = None
        try:
            if mode == 'oversized greeting':
                result = stream_oversized(conn, 0, b'\x0a')
            else:
                scramble = b'0123456789abcdefghij'
                greeting = (b'\x0a' + b'8.0.0-fake\0' + struct.pack('<I', 1) + scramble[:8] + b'\0'
                    + struct.pack('<H', CAPABILITIES & 0xffff) + b'\x21' + struct.pack('<H', 2)
                    + struct.pack('<H', CAPABILITIES >> 16) + bytes([21]) + b'\0' * 10
                    + scramble[8:] + b'\0' + b'mysql_native_password\0')
                conn.sendall(packet(0, greeting))
                # The proxy must not log in for a client whose auth-switch response it rejected.
                try:
                    read_packet(conn)
                    result = 'login received'
                except (EOFError, OSError):
                    result = 'no login'
        except OSError:
            pass
        conn.close()
        with backend_done:
            backend_results.append(result)
            backend_done.notify_all()

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
    config = f'{tmp}/05843_proxy_config.xml'
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
            <rule><user>user_05843</user><pool>my</pool></rule>
        </rules>
        <health_check><enabled>0</enabled></health_check>
    </proxy>
</clickhouse>
''')
    log = f'{tmp}/05843_proxy.log'
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

def connect_and_log_in():
    conn = socket.create_connection(('127.0.0.1', port), timeout=60)
    read_packet(conn)   # The greeting of the proxy.
    response = (struct.pack('<IIB', CAPABILITIES, 1 << 24, 33) + b'\0' * 23 + b'user_05843\0'
        + bytes([20]) + b'x' * 20 + b'mysql_native_password\0')
    conn.sendall(packet(1, response))
    return conn

def wait_for_backend():
    with backend_done:
        if not backend_done.wait_for(lambda: backend_results, timeout=60):
            return 'the backend connection did not end'
        return backend_results.pop()

try:
    conn = connect_and_log_in()
    accepted = wait_for_backend()
    print('oversized greeting of the backend:',
        'rejected' if isinstance(accepted, int) and accepted < REJECTED_BELOW else f'not rejected ({accepted})')
    conn.close()

    mode = 'oversized auth-switch response'
    conn = connect_and_log_in()
    sequence_id, payload = read_packet(conn)
    print('auth-switch request:', payload[:1] == b'\xfe')
    accepted = stream_oversized(conn, sequence_id + 1, b'x')
    print('oversized auth-switch response of the client:',
        'rejected' if accepted < REJECTED_BELOW else f'not rejected ({accepted})')
    conn.close()
    print('backend:', wait_for_backend())
finally:
    proc.kill()
    proc.wait()
EOF
