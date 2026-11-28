#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: clickhouse-proxy is built on the silk fiber framework, which the fast build omits.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A backend serves each protocol on its own port, so its health is tracked per port: a port that does not
# answer takes the backend out of rotation only for the listeners that reach it on this port.
# Here a backend has a dead `http_port` and a live `raw_port`. The connections of the `http` listener are
# refused as soon as the port is marked down, while the `stream` listener keeps reaching the backend.

python3 - "$CLICKHOUSE_BINARY" "$CLICKHOUSE_TMP" <<'EOF'
import json, socket, subprocess, sys, threading, time, urllib.error, urllib.request

binary, tmp = sys.argv[1], sys.argv[2]

def serve(server):
    while True:
        conn, _ = server.accept()
        try:
            conn.sendall(b'hello from the backend')
        except OSError:
            pass    # A probe of the health monitor: it connects and closes.
        conn.close()

live = socket.socket()
live.bind(('127.0.0.1', 0))
live.listen(64)
live_port = live.getsockname()[1]
threading.Thread(target=serve, args=(live,), daemon=True).start()

# Bound, but not listening: connections to it are refused for the whole test.
dead = socket.socket()
dead.bind(('127.0.0.1', 0))
dead_port = dead.getsockname()[1]

def start_proxy():
    # Free ports are picked by binding sockets and releasing them, so another process can take them
    # before the proxy binds them: the caller retries on fresh ports.
    probes = [socket.socket(), socket.socket()]
    for probe in probes:
        probe.bind(('127.0.0.1', 0))
    http_port, stream_port = (probe.getsockname()[1] for probe in probes)
    for probe in probes:
        probe.close()
    config = f'{tmp}/05895_proxy_config.xml'
    with open(config, 'w') as f:
        f.write(f'''<clickhouse>
    <logger><level>information</level><console>1</console></logger>
    <proxy>
        <listen_host>127.0.0.1</listen_host>
        <listeners>
            <listener><protocol>http</protocol><port>{http_port}</port><pool>p</pool></listener>
            <listener><protocol>stream</protocol><port>{stream_port}</port><pool>p</pool></listener>
        </listeners>
        <pools>
            <pool>
                <name>p</name>
                <backend><host>127.0.0.1</host><http_port>{dead_port}</http_port><raw_port>{live_port}</raw_port></backend>
            </pool>
        </pools>
        <http><status_path>/proxy_status</status_path></http>
        <health_check><enabled>1</enabled><interval_ms>100</interval_ms><failures_to_mark_down>2</failures_to_mark_down></health_check>
    </proxy>
</clickhouse>
''')
    log = f'{tmp}/05895_proxy.log'
    proc = subprocess.Popen([binary, 'proxy', '--config-file', config],
        stdout=open(log, 'w'), stderr=subprocess.STDOUT)
    # Wait for our own proxy to report its listeners rather than probing the ports: a probe can be
    # answered by another process that took a port, while our proxy is still starting and fails to bind.
    listening = [f'Listening for http on 127.0.0.1:{http_port}', f'Listening for stream on 127.0.0.1:{stream_port}']
    for _ in range(300):
        if proc.poll() is not None:
            return None, None, None
        with open(log, errors='replace') as f:
            text = f.read()
        if all(line in text for line in listening):
            return proc, http_port, stream_port
        time.sleep(0.1)
    proc.kill()
    proc.wait()
    return None, None, None

proc = None
for _ in range(5):
    proc, http_port, stream_port = start_proxy()
    if proc:
        break
if not proc:
    print('proxy did not start')
    sys.exit(0)

def backend_status():
    with urllib.request.urlopen(f'http://127.0.0.1:{http_port}/proxy_status', timeout=10) as response:
        return json.load(response)['pools'][0]['backends'][0]

def through_stream_listener():
    with socket.create_connection(('127.0.0.1', stream_port), timeout=60) as conn:
        data = b''
        while chunk := conn.recv(4096):
            data += chunk
        return data.decode()

try:
    marked_down = None
    for _ in range(600):
        marked_down = backend_status().get('ports_marked_down')
        if marked_down:
            break
        time.sleep(0.1)
    print('only the dead port is marked down:', marked_down == [dead_port])

    try:
        urllib.request.urlopen(f'http://127.0.0.1:{http_port}/?query=SELECT%201', timeout=60)
        print('http listener: the request was served')
    except urllib.error.HTTPError as e:
        print('http listener:', e.code, e.read().decode().strip())

    # The probes and the refused connections of the dead port go on meanwhile.
    for _ in range(5):
        print('stream listener:', through_stream_listener())
        time.sleep(0.2)

    status = backend_status()
    print('alive:', status['alive'], 'only the dead port is marked down:', status.get('ports_marked_down') == [dead_port])
finally:
    proc.kill()
    proc.wait()
EOF
