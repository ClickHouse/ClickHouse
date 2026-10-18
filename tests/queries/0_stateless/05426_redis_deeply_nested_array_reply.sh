#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A fake Redis server replies to every command with an array nested 100000 levels deep.
# Parsing such a reply must fail with an exception instead of a stack overflow.

PORT_FILE="${CLICKHOUSE_TMP}/redis_port"
rm -f "$PORT_FILE"

python3 -c "
import socket, sys, threading
reply = b'*1\r\n' * 100000 + b':1\r\n'
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind(('127.0.0.1', 0))
server.listen()
server.settimeout(60)
with open(sys.argv[1] + '.tmp', 'w') as f:
    f.write(str(server.getsockname()[1]))
import os
os.rename(sys.argv[1] + '.tmp', sys.argv[1])
def serve(conn):
    with conn:
        try:
            while conn.recv(65536):
                conn.sendall(reply)
        except OSError:
            pass
try:
    while True:
        conn, _ = server.accept()
        threading.Thread(target=serve, args=(conn,), daemon=True).start()
except socket.timeout:
    pass
" "$PORT_FILE" &
SERVER_PID=$!

for _ in {1..100}; do
    [ -f "$PORT_FILE" ] && break
    sleep 0.1
done
PORT=$(cat "$PORT_FILE")

$CLICKHOUSE_CLIENT --query "SELECT * FROM redis('127.0.0.1:${PORT}', 'k', 'k String, v String') WHERE k = 'a'" 2>&1 | grep -q -F 'Too deep nesting of arrays' && echo OK

kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null
rm -f "$PORT_FILE"
