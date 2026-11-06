#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan

# `CREATE TABLE IF NOT EXISTS` in a Unity `DataLakeCatalog` database writes commit 0 and then registers
# the table (`registerDeltaTableInCatalog`). When the registration fails, `IF NOT EXISTS` probes the
# catalog and treats the entry as a no-op only if it points at the same location: an entry at a different
# path stays a conflict, and a probe that itself fails must not replace the original create error.
# The path exercised is the one of `test_database_delta/test.py::test_create_delta_table_in_unity_catalog`,
# with a mock Unity server in place of the catalog so both failure branches are reachable.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_unity"
PORT_FIFO="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.port"
rm -f "$PORT_FIFO"
mkfifo "$PORT_FIFO"

# Unity routes used: `GET /tables/<wh.ns.t>` (the pre-create existence check and the probe),
# `GET /schemas/<wh.ns>` (namespace check) and `POST /tables` (register). The register always answers
# `409 Conflict`, so the probe decides the outcome: `403` for `t_probe_fails`, otherwise an entry at a
# different location. Both statuses are non-retriable, so neither costs an HTTP retry loop.
python3 -u -c "
import json, sys
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse

registered = set()

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
            name = path.split('/tables/', 1)[1].split('.')[-1]
            if name not in registered:
                return self.reply(404, {'error_code': 'TABLE_DOES_NOT_EXIST'})
            if name.endswith('probe_fails'):
                return self.reply(403, {'error_code': 'PERMISSION_DENIED'})
            return self.reply(200, {'name': name, 'data_source_format': 'DELTA',
                                    'storage_location': 'file:///var/empty/elsewhere/'})
        if path.startswith('/schemas'):
            return self.reply(200, {'name': 'ns', 'catalog_name': 'wh', 'full_name': 'wh.ns', 'schemas': []})
        return self.reply(200, {'tables': []})

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
        registered.add(json.loads(raw or b'{}').get('name', ''))
        self.reply(409, {'error_code': 'TABLE_ALREADY_EXISTS'})

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
    rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE}_other" "${CLICKHOUSE_USER_FILES_UNIQUE}_probe_fails"
}
trap cleanup EXIT

# Reading the FIFO is the handshake with the mock: it writes the port only after its socket is bound.
read -r -t 60 PORT < "$PORT_FIFO"

${CLICKHOUSE_CLIENT} --query "
    SET allow_database_unity_catalog = 1;
    CREATE DATABASE ${DB} ENGINE = DataLakeCatalog('http://127.0.0.1:${PORT}')
    SETTINGS catalog_type = 'unity', warehouse = 'wh', allow_experimental_delta_kernel_rs = 1;
"

# Prints the reported error code name, so a probe error leaking out instead of the create error shows up.
register() {
    ${CLICKHOUSE_CLIENT} --query "
        SET allow_experimental_delta_kernel_rs = 1;
        SET allow_delta_lake_writes = 1;
        SET allow_delta_lake_create_table = 1;
        CREATE TABLE IF NOT EXISTS ${DB}.\`ns.t_$1\` (id Int32)
            ENGINE = DeltaLakeLocal('${CLICKHOUSE_USER_FILES_UNIQUE}_$1', Parquet);
    " 2>&1 | grep -o -m1 'DATALAKE_DATABASE_ERROR'
}

echo "catalog entry at another location: $(register other)"
echo "catalog probe refused: $(register probe_fails)"

# Commit 0 is deliberately not rolled back when the registration fails, so the table stays readable.
echo "rows after a failed registration: $(${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM deltaLakeLocal('${CLICKHOUSE_USER_FILES_UNIQUE}_other')
    SETTINGS allow_experimental_delta_kernel_rs = 1")"
