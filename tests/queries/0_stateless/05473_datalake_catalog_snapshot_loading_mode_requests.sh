#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: `DataLakeCatalog` requires the USE_AVRO and USE_PARQUET build options.

# `snapshot_loading_mode` of a `DataLakeCatalog` database is sent as the `snapshots` parameter of the
# Iceberg REST load-table request. The stub catalog records the query string of each load-table request
# (`-` when empty) and answers 404.

# The failing table load is logged by the server, keep it off the client's stderr.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PORT_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.port"
rm -f "$PORT_FILE"

python3 - "$PORT_FILE" <<'EOF' &
import os, sys, threading
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

configs = {
    'adv_refs': b'{"defaults": {"snapshot-loading-mode": "refs"}, "overrides": {}}',
    'adv_none': b'{"defaults": {}, "overrides": {}}',
}
recorded = {}
lock = threading.Lock()

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path, _, query = self.path.partition('?')
        parts = path.strip('/').split('/')
        body, code = b'{}', 404
        if parts == ['ready']:
            code = 200
        elif len(parts) == 2 and parts[0] in configs and parts[1] == 'requests':
            with lock:
                body, code = ' '.join(sorted(recorded.pop(parts[0], {'no request'}))).encode(), 200
        elif len(parts) >= 3 and parts[0] in configs and parts[1] == 'v1':
            if parts[2:] == ['config']:
                body, code = configs[parts[0]], 200
            elif len(parts) == 6 and parts[2] == 'namespaces' and parts[4] == 'tables':
                with lock:
                    recorded.setdefault(parts[0], set()).add(query or '-')
                body = b'{"error": {"message": "Table does not exist", "type": "NoSuchTableException", "code": 404}}'
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass

server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
with open(sys.argv[1] + '.tmp', 'w') as f:
    f.write(str(server.server_address[1]))
os.replace(sys.argv[1] + '.tmp', sys.argv[1])
server.serve_forever()
EOF
HTTP_PID=$!

cleanup() {
    kill $HTTP_PID 2>/dev/null
    for n in 1 2 3 4; do
        ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_${n}" 2>/dev/null
    done
    rm -f "$PORT_FILE"
}
trap cleanup EXIT

for _ in {1..100}; do
    [ -s "$PORT_FILE" ] && break
    sleep 0.1
done
HTTP_PORT=$(cat "$PORT_FILE")
for _ in {1..100}; do
    if [ "$(${CLICKHOUSE_CURL} -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:${HTTP_PORT}/ready")" = "200" ]; then
        break
    fi
    sleep 0.1
done

# <number> <catalog configuration> <snapshot_loading_mode, empty to omit it>
check() {
    local db="${CLICKHOUSE_DATABASE}_$1" settings="catalog_type = 'rest', warehouse = 'wh'"
    if [ -n "$3" ]; then
        settings="${settings}, snapshot_loading_mode = '$3'"
    fi
    ${CLICKHOUSE_CLIENT} --allow_database_iceberg=1 --query "
        CREATE DATABASE ${db} ENGINE = DataLakeCatalog('http://127.0.0.1:${HTTP_PORT}/$2/v1')
        SETTINGS ${settings}"
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${db}.\`ns.t\`" >/dev/null 2>&1
    echo "$2, ${3:-omitted}: $(${CLICKHOUSE_CURL} -s "http://127.0.0.1:${HTTP_PORT}/$2/requests")"
}

check 1 adv_refs ''
check 2 adv_none ''
check 3 adv_refs all
check 4 adv_none refs
