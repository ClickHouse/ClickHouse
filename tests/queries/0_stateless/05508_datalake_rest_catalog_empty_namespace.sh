#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: `DataLakeCatalog` requires the USE_AVRO and USE_PARQUET build options.

# A namespace listing of an Iceberg REST catalog that names no namespace is a malformed catalog response:
# an empty entry, and an empty name at the root (which would denote the root itself).

# The failing listing is logged by the server, keep it off the client's stderr.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PORT_FILE=$(mktemp "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.XXXXXX.port")

python3 -c "
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

namespaces = {
    'empty': b'{\"namespaces\": [[\"good_ns\"], []]}',
    'rootname': b'{\"namespaces\": [[\"\"]]}',
}

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        parts = self.path.split('?')[0].strip('/').split('/')
        body, code = b'{}', 404
        if len(parts) == 3 and parts[0] in namespaces and parts[1] == 'v1':
            if parts[2] == 'config':
                body, code = b'{\"defaults\": {}, \"overrides\": {}}', 200
            elif parts[2] == 'namespaces':
                body, code = namespaces[parts[0]], 200
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass

server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
with open('$PORT_FILE', 'w') as f:
    f.write(str(server.server_address[1]))
server.serve_forever()
" &
HTTP_PID=$!

cleanup() {
    kill $HTTP_PID 2>/dev/null
    wait $HTTP_PID 2>/dev/null
    rm -f "$PORT_FILE"
    for case in empty rootname; do
        ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_${case}" 2>/dev/null
    done
}
trap cleanup EXIT

for _ in {1..300}; do
    [ -s "$PORT_FILE" ] && break
    sleep 0.1
done
HTTP_PORT=$(cat "$PORT_FILE")
[ -n "$HTTP_PORT" ] || { echo 'the catalog server did not start'; exit 1; }

for case in empty rootname; do
    DB="${CLICKHOUSE_DATABASE}_${case}"
    ${CLICKHOUSE_CLIENT} --allow_database_iceberg=1 --query "
        CREATE DATABASE ${DB} ENGINE = DataLakeCatalog('http://127.0.0.1:${HTTP_PORT}/${case}/v1')
        SETTINGS catalog_type = 'rest', warehouse = 'wh'"
    echo "${case}: $(${CLICKHOUSE_CLIENT} --query "SHOW TABLES FROM ${DB}" 2>&1 | grep -o -m1 'DATALAKE_DATABASE_ERROR')"
done
