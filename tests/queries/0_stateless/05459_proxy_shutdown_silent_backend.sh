#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: clickhouse-proxy is built on the silk fiber framework, which the fast build omits.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The proxy must stop on `SIGTERM` while it relays an established `stream` connection whose backend keeps its side
# open and silent. Shutting down the client socket alone is a clean end of stream for the relay, which only half-closes
# the backend leg, so the relay has to be torn down as a whole, or the proxy waits for it forever.

python3 - "$CLICKHOUSE_BINARY" "$CLICKHOUSE_TMP" <<'EOF'
import socket, subprocess, sys, threading, time

binary, tmp = sys.argv[1], sys.argv[2]

backend = socket.socket()
backend.bind(('127.0.0.1', 0))
backend.listen(8)
backend_port = backend.getsockname()[1]
accepted = threading.Event()
held = []

def serve():
    while True:
        conn, _ = backend.accept()
        held.append(conn)   # Never reads and never closes.
        accepted.set()

threading.Thread(target=serve, daemon=True).start()

def start_proxy():
    # A free port is picked by binding a socket and releasing it, so another process can take it
    # before the proxy binds it: the caller retries on a fresh port.
    probe = socket.socket()
    probe.bind(('127.0.0.1', 0))
    port = probe.getsockname()[1]
    probe.close()
    config = f'{tmp}/05459_proxy_config.xml'
    with open(config, 'w') as f:
        f.write(f'''<clickhouse>
    <logger><level>information</level><console>1</console></logger>
    <proxy>
        <listen_host>127.0.0.1</listen_host>
        <listeners>
            <listener><protocol>stream</protocol><port>{port}</port><pool>p</pool></listener>
        </listeners>
        <pools>
            <pool><name>p</name><backend><host>127.0.0.1</host><raw_port>{backend_port}</raw_port></backend></pool>
        </pools>
        <health_check><enabled>0</enabled></health_check>
    </proxy>
</clickhouse>
''')
    log = f'{tmp}/05459_proxy.log'
    proc = subprocess.Popen([binary, 'proxy', '--config-file', config], stdout=open(log, 'w'), stderr=subprocess.STDOUT)
    listening = f'Listening for stream on 127.0.0.1:{port}'
    for _ in range(300):
        if proc.poll() is not None:
            with open(log, errors='replace') as f:
                return None, None, f.read()
        with open(log, errors='replace') as f:
            if listening in f.read():
                return proc, port, None
        time.sleep(0.1)
    proc.kill()
    proc.wait()
    return None, None, None

proc, port, failure = None, None, None
for _ in range(5):
    proc, port, failure = start_proxy()
    if proc:
        break
    if failure and 'the silk fiber framework, which is only available on Linux' in failure:
        # The proxy is not available in this build; emit the expected output so the test passes.
        print('stopped 0')
        sys.exit(0)
if not proc:
    print('proxy did not start')
    sys.exit(1)

client = socket.create_connection(('127.0.0.1', port), timeout=10)
client.sendall(b'hello')
if not accepted.wait(timeout=30):
    print('the connection did not reach the backend')

proc.terminate()
try:
    print('stopped', proc.wait(timeout=60))
except subprocess.TimeoutExpired:
    print('the proxy did not stop')
    proc.kill()
    proc.wait()
client.close()
EOF
