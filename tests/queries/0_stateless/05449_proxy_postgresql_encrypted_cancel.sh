#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: clickhouse-proxy is built on the silk fiber framework, which the fast build omits.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The proxy cannot read the `CancelRequest` of an encrypted PostgreSQL session (it is sent after an
# `SSLRequest` on a fresh connection), so it cannot broadcast it like a cleartext one. Connections that
# begin with an encryption request must therefore reach the same backend, whatever the pool's load
# balancing: here, two fake backends behind `round_robin` see every encrypted connection on one of
# them, while cleartext `StartupMessage` connections alternate between them.

python3 - "$CLICKHOUSE_BINARY" "$CLICKHOUSE_TMP" <<'EOF'
import socket, struct, subprocess, sys, threading, time

binary, tmp = sys.argv[1], sys.argv[2]

hits = {}
lock = threading.Lock()
received = threading.Condition(lock)

def serve(name, server):
    while True:
        conn, _ = server.accept()
        conn.settimeout(10)
        try:
            data = b''
            while len(data) < 8:
                chunk = conn.recv(8 - len(data))
                if not chunk:
                    break
                data += chunk
        except OSError:
            pass
        conn.close()
        with received:
            hits[name] = hits.get(name, 0) + 1
            received.notify_all()

backend_ports = []
for name in ('A', 'B'):
    server = socket.socket()
    server.bind(('127.0.0.1', 0))
    server.listen(64)
    backend_ports.append(server.getsockname()[1])
    threading.Thread(target=serve, args=(name, server), daemon=True).start()

def start_proxy():
    # A free port is picked by binding a socket and releasing it, so another process can take it
    # before the proxy binds it: the caller retries on a fresh port.
    probe = socket.socket()
    probe.bind(('127.0.0.1', 0))
    port = probe.getsockname()[1]
    probe.close()
    config = f'{tmp}/05449_proxy_config.xml'
    with open(config, 'w') as f:
        f.write(f'''<clickhouse>
    <logger><level>warning</level><console>1</console></logger>
    <proxy>
        <listen_host>127.0.0.1</listen_host>
        <listeners>
            <listener><protocol>postgresql</protocol><port>{port}</port><pool>pg</pool></listener>
        </listeners>
        <pools>
            <pool>
                <name>pg</name>
                <load_balancing>round_robin</load_balancing>
                <backend><host>127.0.0.1</host><postgresql_port>{backend_ports[0]}</postgresql_port></backend>
                <backend><host>127.0.0.1</host><postgresql_port>{backend_ports[1]}</postgresql_port></backend>
            </pool>
        </pools>
        <health_check><enabled>0</enabled></health_check>
    </proxy>
</clickhouse>
''')
    proc = subprocess.Popen([binary, 'proxy', '--config-file', config],
        stdout=open(f'{tmp}/05449_proxy.log', 'w'), stderr=subprocess.STDOUT)
    for _ in range(100):
        if proc.poll() is not None:
            return None, None
        try:
            socket.create_connection(('127.0.0.1', port), timeout=1).close()
            # The probe connection is closed without data and reaches no backend.
            return proc, port
        except OSError:
            time.sleep(0.2)
    proc.kill()
    return None, None

proc, port = None, None
for _ in range(5):
    proc, port = start_proxy()
    if proc:
        break
if not proc:
    print('proxy did not start')
    sys.exit(0)

def run(packet, count):
    with received:
        hits.clear()
    for _ in range(count):
        conn = socket.create_connection(('127.0.0.1', port), timeout=10)
        conn.sendall(packet)
        with received:
            expected = sum(hits.values()) + 1
            received.wait_for(lambda: sum(hits.values()) >= expected, timeout=30)
        conn.close()
    with received:
        return sorted(hits.values())

try:
    ssl_request = struct.pack('>II', 8, 80877103)
    # An encrypted session followed by the encrypted connections of its cancellations.
    print('encrypted', run(ssl_request, 6))

    body = struct.pack('>I', 196608) + b'user\0default\0\0'
    startup = struct.pack('>I', len(body) + 4) + body
    print('cleartext', run(startup, 6))
finally:
    proc.kill()
    proc.wait()
EOF
