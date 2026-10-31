#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An error code the client does not know (e.g. QUOTAEXCEEDED, -125) must be reported as an error, not crash the client.
# The fake ZooKeeper below answers a request for the path `/code_<N>` with the error code N.

FAKE_ZK_SCRIPT="${CLICKHOUSE_TMP}/05457_fakezk_${CLICKHOUSE_DATABASE}.py"
FAKE_ZK_PORT_FILE="${CLICKHOUSE_TMP}/05457_fakezk_${CLICKHOUSE_DATABASE}.port"

rm -f "$FAKE_ZK_PORT_FILE"

cat > "$FAKE_ZK_SCRIPT" <<'EOF'
import os
import socket
import struct
import sys
import threading

PING_XID = -2
AUTH_XID = -4
CLOSE_OP = -11
ZNONODE = -101
CODE_PREFIX = "/code_"

port_file = sys.argv[1]


def read_exactly(conn, size):
    data = b""
    while len(data) < size:
        chunk = conn.recv(size - len(data))
        if not chunk:
            raise EOFError
        data += chunk
    return data


def read_packet(conn):
    (size,) = struct.unpack(">i", read_exactly(conn, 4))
    return read_exactly(conn, size)


def send_packet(conn, body):
    conn.sendall(struct.pack(">i", len(body)) + body)


def error_for(xid, op, request):
    if xid in (PING_XID, AUTH_XID) or op == CLOSE_OP:
        return 0
    if len(request) >= 12:
        (size,) = struct.unpack(">i", request[8:12])
        if 0 <= size <= len(request) - 12:
            path = request[12 : 12 + size].decode(errors="replace")
            if path.startswith(CODE_PREFIX):
                return int(path[len(CODE_PREFIX) :])
    return ZNONODE


def serve(conn):
    try:
        (protocol_version,) = struct.unpack(">i", read_packet(conn)[:4])
        send_packet(conn, struct.pack(">iiqi", protocol_version, 30000, 0x1234, 16) + b"\0" * 16)
        while True:
            request = read_packet(conn)
            xid, op = struct.unpack(">ii", request[:8])
            send_packet(conn, struct.pack(">iqi", xid, 1, error_for(xid, op, request)))
    except (EOFError, OSError, ValueError):
        pass
    finally:
        conn.close()


listener = socket.socket()
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("127.0.0.1", 0))
listener.listen(16)

with open(port_file + ".tmp", "w") as f:
    f.write(str(listener.getsockname()[1]))
os.rename(port_file + ".tmp", port_file)

while True:
    connection, _ = listener.accept()
    threading.Thread(target=serve, args=(connection,), daemon=True).start()
EOF

python3 "$FAKE_ZK_SCRIPT" "$FAKE_ZK_PORT_FILE" 2>/dev/null &
FAKE_ZK_PID=$!
# shellcheck disable=SC2064
trap "kill $FAKE_ZK_PID 2>/dev/null; rm -f '$FAKE_ZK_SCRIPT' '$FAKE_ZK_PORT_FILE'" EXIT

for _ in {1..300}; do
    [ -s "$FAKE_ZK_PORT_FILE" ] && break
    sleep 0.1
done

FAKE_ZK_PORT=$(cat "$FAKE_ZK_PORT_FILE")

if [ -x "${CLICKHOUSE_BINARY}-keeper-client" ]; then
    KEEPER_CLIENT="${CLICKHOUSE_BINARY}-keeper-client"
else
    KEEPER_CLIENT="${CLICKHOUSE_BINARY} keeper-client"
fi

for code in -101 -121 -120 -125 -13; do
    # shellcheck disable=SC2086
    out=$($KEEPER_CLIENT --host 127.0.0.1 --port "$FAKE_ZK_PORT" --connection-timeout 10 --session-timeout 30 --operation-timeout 10 -q "ls '/code_$code'" 2>&1)
    rc=$?
    echo "$out"
    echo "rc=$rc"
done
