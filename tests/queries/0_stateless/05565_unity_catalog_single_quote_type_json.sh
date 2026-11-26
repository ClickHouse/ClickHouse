#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# - no-fasttest: `DataLakeCatalog` requires the USE_AVRO and USE_PARQUET build options.
# - no-msan: delta-kernel-rs is not built with MSan, so Unity catalog Delta tables are unreadable there.

# A Unity catalog column whose `type_json` is a single `"` is a malformed catalog response.

# The failing schema is logged by the server, keep it off the client's stderr.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PORT_FILE=$(mktemp "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.XXXXXX.port")

python3 -c "
import json
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

table = json.dumps({
    'name': 't', 'data_source_format': 'DELTA', 'storage_location': 's3://bucket/t',
    'columns': [{'name': 'c', 'nullable': True, 'type_json': '\"'}]}).encode()

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path = self.path.split('?')[0]
        body, code = b'{}', 404
        if path == '/api/2.1/unity-catalog/tables/wh.s.t':
            body, code = table, 200
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

DB="${CLICKHOUSE_DATABASE}_unity"

cleanup() {
    kill $HTTP_PID 2>/dev/null
    wait $HTTP_PID 2>/dev/null
    rm -f "$PORT_FILE"
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB}" 2>/dev/null
}
trap cleanup EXIT

for _ in {1..300}; do
    [ -s "$PORT_FILE" ] && break
    sleep 0.1
done
HTTP_PORT=$(cat "$PORT_FILE")
[ -n "$HTTP_PORT" ] || { echo 'the catalog server did not start'; exit 1; }

${CLICKHOUSE_CLIENT} --allow_database_unity_catalog=1 --query "
    CREATE DATABASE ${DB} ENGINE = DataLakeCatalog('http://127.0.0.1:${HTTP_PORT}/api/2.1/unity-catalog')
    SETTINGS catalog_type = 'unity', warehouse = 'wh'"
${CLICKHOUSE_CLIENT} --query "DESCRIBE TABLE ${DB}.\`s.t\`" 2>&1 | grep -o -m1 -E 'Unsupported DeltaLake type|STD_EXCEPTION|out_of_range'
