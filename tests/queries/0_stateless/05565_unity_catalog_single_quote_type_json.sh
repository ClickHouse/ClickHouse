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

HTTP_PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

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
        if path == '/ready':
            code = 200
        elif path == '/api/2.1/unity-catalog/tables/wh.s.t':
            body, code = table, 200
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass

ThreadingHTTPServer(('127.0.0.1', $HTTP_PORT), Handler).serve_forever()
" &
HTTP_PID=$!

DB="${CLICKHOUSE_DATABASE}_unity"

cleanup() {
    kill $HTTP_PID 2>/dev/null
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB}" 2>/dev/null
}
trap cleanup EXIT

for _ in {1..100}; do
    if [ "$(${CLICKHOUSE_CURL} -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:${HTTP_PORT}/ready")" = "200" ]; then
        break
    fi
    sleep 0.1
done

${CLICKHOUSE_CLIENT} --allow_database_unity_catalog=1 --query "
    CREATE DATABASE ${DB} ENGINE = DataLakeCatalog('http://127.0.0.1:${HTTP_PORT}/api/2.1/unity-catalog')
    SETTINGS catalog_type = 'unity', warehouse = 'wh'"
${CLICKHOUSE_CLIENT} --query "DESCRIBE TABLE ${DB}.\`s.t\`" 2>&1 | grep -o -m1 -E 'Unsupported DeltaLake type|STD_EXCEPTION|out_of_range'
