#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: `DataLakeCatalog` requires the USE_AVRO and USE_PARQUET build options.

# The listing of a Unity catalog skips the schemas the server says belong to another catalog or to none, and
# fails when a page repeats a `next_page_token`, instead of requesting the same page forever. Both Unity
# implementations.

# The failing queries are logged by the server, keep it off the client's stderr.
CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PORT_FILE=$(mktemp "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.XXXXXX.port")

# A catalog `loop_*` pages its listing through the token cycle `A -> B -> A`; the fourth request of a listing gets
# no token, so a client that does not stop at the first repeated token still finishes.
python3 -c "
import json
import threading
from urllib.parse import urlsplit, parse_qs
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

table = {'name': 't', 'data_source_format': 'DELTA', 'storage_location': 's3://bucket/t'}
schemas = {
    'wh': {'schemas': [
        {'name': 's', 'catalog_name': 'wh', 'full_name': 'wh.s'},
        {'name': 'upper', 'catalog_name': 'WH', 'full_name': 'WH.upper'},
        {'name': 'foreign', 'catalog_name': 'other', 'full_name': 'wh.foreign'},
        {'name': 'nocat', 'full_name': 'wh.nocat'}]},
    'loop_schemas': {'schemas': [], 'next_page_token': 'A'},
    'loop_tables': {'schemas': [{'name': 's', 'catalog_name': 'loop_tables', 'full_name': 'loop_tables.s'}]},
}
repeats = {}
lock = threading.Lock()

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        url = urlsplit(self.path)
        params = parse_qs(url.query)
        catalog = params.get('catalog_name', [''])[0]
        body, code = {}, 404
        if url.path == '/api/2.1/unity-catalog/schemas' and catalog in schemas:
            body, code = dict(schemas[catalog]), 200
        elif url.path == '/api/2.1/unity-catalog/tables':
            body, code = {'tables': [table]}, 200
            if catalog == 'loop_tables':
                body['next_page_token'] = 'A'
        if 'next_page_token' in body:
            key = (url.path, catalog, params.get('schema_name', [''])[0])
            with lock:
                repeats[key] = repeats.get(key, 0) + 1 if 'page_token' in params else 0
                if repeats[key] >= 3:
                    del body['next_page_token']
                else:
                    body['next_page_token'] = 'B' if params.get('page_token', [''])[0] == 'A' else 'A'
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

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
    for v2 in 0 1; do
        for wh in wh loop_schemas loop_tables; do
            ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB}_${wh}_${v2}" 2>/dev/null
        done
    done
}
trap cleanup EXIT

for _ in {1..300}; do
    [ -s "$PORT_FILE" ] && break
    sleep 0.1
done
HTTP_PORT=$(cat "$PORT_FILE")
[ -n "$HTTP_PORT" ] || { echo 'the catalog server did not start'; exit 1; }

for v2 in 0 1; do
    echo "use_unity_catalog_v2 = ${v2}"
    for wh in wh loop_schemas loop_tables; do
        ${CLICKHOUSE_CLIENT} --allow_database_unity_catalog=1 --query "
            CREATE DATABASE ${DB}_${wh}_${v2} ENGINE = DataLakeCatalog('http://127.0.0.1:${HTTP_PORT}/api/2.1/unity-catalog')
            SETTINGS catalog_type = 'unity', warehouse = '${wh}', use_unity_catalog_v2 = ${v2}, vended_credentials = 0"
    done
    ${CLICKHOUSE_CLIENT} --query "SHOW TABLES FROM ${DB}_wh_${v2}"
    for wh in loop_schemas loop_tables; do
        result=$(${CLICKHOUSE_CLIENT} --query "SHOW TABLES FROM ${DB}_${wh}_${v2}" 2>&1)
        echo "${wh}: $(grep -o -m1 -E 'twice while listing the (schemas|tables)' <<< "$result")" \
            "$(grep -o -m1 -E 'DATALAKE_DATABASE_ERROR|POCO_EXCEPTION' <<< "$result")"
    done
done
