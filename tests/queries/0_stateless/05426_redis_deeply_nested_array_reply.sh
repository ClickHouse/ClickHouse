#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A fake Redis server replies to a command mentioning the key 'deep' with an array nested 100000 levels deep,
# and to any other command with an array of nils (one per argument except the command name).
# Parsing the deep reply must fail with an exception instead of a stack overflow,
# and the connection must not be reused: the next query over the same single-connection pool
# must receive its own reply instead of the unread remainder of the deep one.

PORT_FILE="${CLICKHOUSE_TMP}/redis_port"
rm -f "$PORT_FILE"

python3 -c "
import os, socket, sys, threading
deep_reply = b'*1\r\n' * 100000 + b':1\r\n'
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind(('127.0.0.1', 0))
server.listen()
server.settimeout(60)
with open(sys.argv[1] + '.tmp', 'w') as f:
    f.write(str(server.getsockname()[1]))
os.rename(sys.argv[1] + '.tmp', sys.argv[1])

def serve(conn):
    reader = conn.makefile('rb')
    with conn:
        try:
            while True:
                header = reader.readline()
                if not header:
                    return
                num_args = int(header[1:])
                args = []
                for _ in range(num_args):
                    length = int(reader.readline()[1:])
                    args.append(reader.read(length + 2)[:-2])
                if any(b'deep' in arg for arg in args):
                    conn.sendall(deep_reply)
                else:
                    conn.sendall(b'*%d\r\n' % (num_args - 1) + b'\$-1\r\n' * (num_args - 1))
        except (OSError, ValueError):
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

$CLICKHOUSE_CLIENT --query "SELECT * FROM redis('127.0.0.1:${PORT}', 'k', 'k String, v String') WHERE k = 'deep'" 2>&1 | grep -q -F 'Too deep nesting of arrays' && echo OK

$CLICKHOUSE_CLIENT --query "CREATE TABLE redis_deep (k String, v String) ENGINE = Redis('127.0.0.1:${PORT}', 0, '', 1) PRIMARY KEY k"
$CLICKHOUSE_CLIENT --query "SELECT * FROM redis_deep WHERE k = 'deep'" 2>&1 | grep -q -F 'Too deep nesting of arrays' && echo OK
$CLICKHOUSE_CLIENT --query "SELECT count() FROM redis_deep WHERE k = 'a'"
$CLICKHOUSE_CLIENT --query "DROP TABLE redis_deep"

kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null
rm -f "$PORT_FILE"
