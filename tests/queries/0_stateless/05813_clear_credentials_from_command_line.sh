#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: `--jwt` and `--oauth-client-secret` need a build with jwt-cpp and SSL.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The values of the options carrying credentials must not be visible in `ps` and `/proc/<pid>/cmdline`.
# The programs connect to a listener that never answers, so they stay alive after parsing their options.

LISTENER_SCRIPT="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_listener.py"
PORT_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.port"
ACCEPTED_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.accepted"
rm -f "$PORT_FILE"
: > "$ACCEPTED_FILE"

cat > "$LISTENER_SCRIPT" <<'EOF'
import os
import socket
import sys

listener = socket.socket()
listener.bind(("127.0.0.1", 0))
listener.listen(16)
with open(sys.argv[1] + ".tmp", "w") as f:
    f.write(str(listener.getsockname()[1]))
os.rename(sys.argv[1] + ".tmp", sys.argv[1])

connections = []
while True:
    connection, _ = listener.accept()
    connections.append(connection)
    with open(sys.argv[2], "a") as f:
        f.write("accepted\n")
EOF

python3 "$LISTENER_SCRIPT" "$PORT_FILE" "$ACCEPTED_FILE" &
LISTENER_PID=$!
trap 'kill $LISTENER_PID 2>/dev/null; wait $LISTENER_PID 2>/dev/null' EXIT

while [ ! -s "$PORT_FILE" ] && kill -0 "$LISTENER_PID" 2>/dev/null; do sleep 0.1; done
PORT=$(cat "$PORT_FILE")

if [ -x "${CLICKHOUSE_BINARY}-keeper-client" ]; then
    KEEPER_CLIENT="${CLICKHOUSE_BINARY}-keeper-client"
else
    KEEPER_CLIENT="${CLICKHOUSE_BINARY} keeper-client"
fi

CONNECTIONS=0
function show_credential_options()
{
    "$@" > /dev/null 2>&1 &
    local pid=$!
    CONNECTIONS=$((CONNECTIONS + 1))
    # A connected program has parsed its options.
    while [ "$(wc -l < "$ACCEPTED_FILE")" -lt "$CONNECTIONS" ] && kill -0 "$pid" 2>/dev/null; do sleep 0.1; done
    tr '\0' '\n' < "/proc/$pid/cmdline" | grep -E '^--(password|jwt|ssh-key-passphrase|one-time-password|oauth-client-secret|identity)|SECRET'
    kill "$pid" 2>/dev/null
    wait "$pid" 2>/dev/null || true
}

JWT='eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJ0ZXN0In0.SECRET'

echo "client, separate values"
show_credential_options ${CLICKHOUSE_CLIENT_BINARY} --host 127.0.0.1 --port "$PORT" --query "SELECT 1" \
    --jwt "$JWT" --password SECRET --ssh-key-passphrase SECRET --one-time-password SECRET --oauth-client-secret SECRET

echo "client, values after ="
show_credential_options ${CLICKHOUSE_CLIENT_BINARY} --host 127.0.0.1 --port "$PORT" --query "SELECT 1" \
    --jwt="$JWT" --password=SECRET --ssh-key-passphrase=SECRET --one-time-password=SECRET --oauth-client-secret=SECRET

echo "client, values after spaced ="
show_credential_options ${CLICKHOUSE_CLIENT_BINARY} --host 127.0.0.1 --port "$PORT" --query "SELECT 1" \
    --jwt = "$JWT" --password = SECRET --ssh-key-passphrase = SECRET --one-time-password = SECRET --oauth-client-secret = SECRET

echo "client, --password without a value"
show_credential_options ${CLICKHOUSE_CLIENT_BINARY} --host 127.0.0.1 --port "$PORT" --query "SELECT 1" \
    --password --jwt "$JWT"

echo "keeper-client"
show_credential_options ${KEEPER_CLIENT} --host 127.0.0.1 --port "$PORT" --connection-timeout 60 --session-timeout 60 \
    --operation-timeout 60 --query "ls" --password SECRET --identity=user:SECRET

echo "keeper-client, values after :"
show_credential_options ${KEEPER_CLIENT} --host 127.0.0.1 --port "$PORT" --connection-timeout 60 --session-timeout 60 \
    --operation-timeout 60 --query "ls" --password:SECRET --identity:user:SECRET
