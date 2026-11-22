#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A malformed HTTP response is not retried: the server would send the same response again.
# A response cut short is still retried.

SERVER_SCRIPT="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_server.py"
PORT_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_port"
rm -f "$PORT_FILE"

cat > "$SERVER_SCRIPT" <<'PYEOF'
import collections, os, socket, sys, threading

counts = collections.Counter()
lock = threading.Lock()

def handle(conn):
    with conn:
        data = b''
        while b'\r\n\r\n' not in data:
            chunk = conn.recv(65536)
            if not chunk:
                return
            data += chunk
        path = data.split(b' ', 2)[1].decode()
        if path.startswith('/count/'):
            with lock:
                body = str(counts[path[len('/count'):]]).encode()
            conn.sendall(b'HTTP/1.1 200 OK\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s' % (len(body), body))
            return
        with lock:
            counts[path] += 1
        if path == '/malformed':
            # A bare CR inside a header value.
            conn.sendall(b'HTTP/1.1 200 OK\r\nX-Bad: a\rb\r\nContent-Length: 5\r\n\r\nhello')
        elif path == '/truncated':
            # The connection is closed in the middle of the status line.
            conn.sendall(b'HTTP/1.1 200')

server = socket.socket()
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind(('127.0.0.1', 0))
server.listen(64)
with open(sys.argv[1] + '.tmp', 'w') as f:
    f.write(str(server.getsockname()[1]))
os.rename(sys.argv[1] + '.tmp', sys.argv[1])
while True:
    conn, _ = server.accept()
    threading.Thread(target=handle, args=(conn,), daemon=True).start()
PYEOF

python3 "$SERVER_SCRIPT" "$PORT_FILE" &
SERVER_PID=$!
trap 'kill $SERVER_PID 2>/dev/null; wait $SERVER_PID 2>/dev/null; rm -f "$SERVER_SCRIPT" "$PORT_FILE"' EXIT

for _ in $(seq 1 100); do
    [ -f "$PORT_FILE" ] && break
    sleep 0.1
done
PORT=$(cat "$PORT_FILE")

SETTINGS="http_make_head_request = 0, http_max_tries = 3, http_retry_initial_backoff_ms = 1, http_retry_max_backoff_ms = 10"

echo "malformed"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM url('http://127.0.0.1:$PORT/malformed', RawBLOB) SETTINGS $SETTINGS" 2>&1 | grep -o -m1 'Malformed message: Field value too long/no CRLF found'
${CLICKHOUSE_CURL} -sS "http://127.0.0.1:$PORT/count/malformed"
echo

echo "truncated"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM url('http://127.0.0.1:$PORT/truncated', RawBLOB) SETTINGS $SETTINGS" 2>&1 | grep -o -m1 'Malformed message: Invalid HTTP status code'
${CLICKHOUSE_CURL} -sS "http://127.0.0.1:$PORT/count/truncated"
echo
