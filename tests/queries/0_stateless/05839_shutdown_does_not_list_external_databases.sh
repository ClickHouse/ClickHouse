#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: `DataLakeCatalog` requires the USE_AVRO and USE_PARQUET build options.

# On shutdown the tables of every database are listed to flush `Buffer` tables. External databases cannot
# hold one, and listing them is a request to their remote side. Both databases below point at a listener
# that only counts connections: `clickhouse-local` attaches them, which connects to nothing, and shuts down.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
PORT_FILE="${DIR}.port"
CONNECTIONS_FILE="${DIR}.connections"
rm -rf "${DIR}" "${PORT_FILE}" "${CONNECTIONS_FILE}"
mkdir -p "${DIR}"
touch "${CONNECTIONS_FILE}"

python3 - "${PORT_FILE}" "${CONNECTIONS_FILE}" <<'EOF' &
import os, socket, sys
listener = socket.socket()
listener.bind(('127.0.0.1', 0))
listener.listen(64)
with open(sys.argv[1] + '.tmp', 'w') as f:
    f.write(str(listener.getsockname()[1]))
os.replace(sys.argv[1] + '.tmp', sys.argv[1])
while True:
    connection, _ = listener.accept()
    with open(sys.argv[2], 'a') as f:
        f.write('connection\n')
    connection.close()
EOF
LISTENER_PID=$!

cleanup() {
    kill ${LISTENER_PID} 2>/dev/null
    rm -rf "${DIR}" "${PORT_FILE}" "${CONNECTIONS_FILE}"
}
trap cleanup EXIT

for _ in {1..100}; do
    [ -s "${PORT_FILE}" ] && break
    sleep 0.1
done
PORT=$(cat "${PORT_FILE}")

${CLICKHOUSE_LOCAL} --path "${DIR}" --query "
    ATTACH DATABASE lake ENGINE = DataLakeCatalog('http://127.0.0.1:${PORT}/v1') SETTINGS catalog_type = 'rest', warehouse = 'wh';
    ATTACH DATABASE proxy ENGINE = Remote('127.0.0.1:${PORT}', 'default');
" 2>&1 | grep -av "ASan doesn't fully support makecontext/swapcontext functions"
echo "exit code: ${PIPESTATUS[0]}"
echo "connections: $(wc -l < "${CONNECTIONS_FILE}")"
