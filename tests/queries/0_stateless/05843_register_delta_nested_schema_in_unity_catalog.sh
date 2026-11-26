#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan

# `CREATE TABLE` in a Unity `DataLakeCatalog` database writes commit 0 and then registers the table,
# serialising the Delta schema it reads back from the `_delta_log` into the catalog entry. Nested
# columns follow the Delta schema serialization format
# (https://github.com/delta-io/delta/blob/master/PROTOCOL.md#schema-serialization-format): an array
# becomes `elementType`/`containsNull`, a map becomes `keyType`/`valueType`/`valueContainsNull`, a
# named tuple becomes a nested `struct` with its own field list. A mock Unity server stands in for the
# catalog and answers `GET /tables/<name>` with exactly the registered entry, so that entry is the
# only source of the types `DESCRIBE` decodes back.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_unity"
PORT_FIFO="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.port"
rm -f "$PORT_FIFO"
mkfifo "$PORT_FIFO"

# Unity routes used: `GET /tables/<wh.ns.t>` (the pre-create existence check, then the read),
# `GET /schemas/<wh.ns>` (the namespace check that disambiguates the 404) and `POST /tables`, which
# stores the body and answers 200, so a later `GET` returns the registered schema byte for byte.
python3 -u -c "
import json, sys
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse

tables = {}

class Handler(BaseHTTPRequestHandler):
    def reply(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = urlparse(self.path).path
        if path.startswith('/tables/'):
            full_name = path.split('/tables/', 1)[1]
            if full_name not in tables:
                return self.reply(404, {'error_code': 'TABLE_DOES_NOT_EXIST'})
            return self.reply(200, tables[full_name])
        if path.startswith('/schemas'):
            return self.reply(200, {'name': 'ns', 'catalog_name': 'wh', 'full_name': 'wh.ns', 'schemas': []})
        return self.reply(200, {'tables': list(tables.values())})

    def do_POST(self):
        if 'chunked' in self.headers.get('Transfer-Encoding', '').lower():
            chunks = []
            while True:
                size = int(self.rfile.readline().split(b';')[0], 16)
                if size == 0:
                    self.rfile.readline()
                    break
                chunks.append(self.rfile.read(size))
                self.rfile.read(2)
            raw = b''.join(chunks)
        else:
            raw = self.rfile.read(int(self.headers.get('Content-Length') or 0))
        table = json.loads(raw or b'{}')
        key = '.'.join([str(table.get(k, '')) for k in ('catalog_name', 'schema_name', 'name')])
        tables[key] = table
        self.reply(200, table)

    def log_message(self, *args):
        pass

server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
with open(sys.argv[1], 'w') as f:
    f.write(str(server.server_address[1]) + '\n')
server.serve_forever()
" "$PORT_FIFO" &
MOCK_PID=$!

cleanup() {
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB}" 2>/dev/null
    kill "$MOCK_PID" 2>/dev/null
    rm -f "$PORT_FIFO"
    rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE}"
}
trap cleanup EXIT

# Reading the FIFO is the handshake with the mock: it writes the port only after its socket is bound.
read -r -t 60 PORT < "$PORT_FIFO"

# `vended_credentials = 0`: a `file://` location needs no storage credentials from the catalog.
${CLICKHOUSE_CLIENT} --query "
    SET allow_database_unity_catalog = 1;
    CREATE DATABASE ${DB} ENGINE = DataLakeCatalog('http://127.0.0.1:${PORT}')
    SETTINGS catalog_type = 'unity', warehouse = 'wh', vended_credentials = 0,
             allow_experimental_delta_kernel_rs = 1;
"

${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_delta_kernel_rs = 1;
    SET allow_delta_lake_writes = 1;
    SET allow_delta_lake_create_table = 1;
    CREATE TABLE ${DB}.\`ns.t\`
    (
        arr Array(Int32),
        m Map(String, Nullable(Int64)),
        s Tuple(x Int32, y Array(String))
    )
    ENGINE = DeltaLakeLocal('${CLICKHOUSE_USER_FILES_UNIQUE}', Parquet);
"

# Each printed type is the round trip of one registered descriptor: a wrong `containsNull` flips the
# array element's `Nullable`, a wrong `valueContainsNull` the map value's, and a `struct` registered
# without its field list does not parse back at all. `print_pretty_type_names = 0`: one line per type.
${CLICKHOUSE_CLIENT} --print_pretty_type_names 0 --query "DESCRIBE TABLE ${DB}.\`ns.t\`" | cut -f1,2

echo "rows: $(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${DB}.\`ns.t\`")"
