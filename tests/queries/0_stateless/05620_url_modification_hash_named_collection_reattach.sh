#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: starts a local HTTP server with python3.
# no-replicated-database: `DETACH` / `ATTACH` of the table and a named collection that is not replicated.

# `system.tables.modification_hash` of a `URL` table must cover how the resource is decoded. A
# `DETACH` / `ATTACH` keeps the UUID of the table, while the configuration is loaded anew: after the
# format in the named collection changes from `CSV` to `TSV`, the same resource with the same strong
# `ETag` is read as different rows, so the hash must change.

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
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

class Handler(BaseHTTPRequestHandler):
    def respond(self, with_body):
        if self.path == '/data':
            code, headers, body = 200, {'ETag': '\"stable\"'}, b'\"a\"\n'
        else:
            code, headers, body = 404, {}, b''
        self.send_response(code)
        for k, v in headers.items():
            self.send_header(k, v)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        if with_body:
            self.wfile.write(body)
    def do_GET(self):
        self.respond(True)
    def do_HEAD(self):
        self.respond(False)
    def log_message(self, *args):
        pass

ThreadingHTTPServer(('127.0.0.1', $HTTP_PORT), Handler).serve_forever()
" &
HTTP_PID=$!
trap "kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null" EXIT

for _ in $(seq 1 100); do
    curl -s "http://127.0.0.1:$HTTP_PORT/data" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

NC="nc_${CLICKHOUSE_DATABASE}_05620"

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_url_nc;
    DROP NAMED COLLECTION IF EXISTS ${NC};
    CREATE NAMED COLLECTION ${NC} AS url = 'http://127.0.0.1:$HTTP_PORT/data', format = 'CSV';
    CREATE TABLE t_url_nc (x String) ENGINE = URL(${NC});
"

function hash()
{
    ${CLICKHOUSE_CLIENT} -q "SELECT modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_url_nc'"
}

${CLICKHOUSE_CLIENT} -q "SELECT x FROM t_url_nc"
${CLICKHOUSE_CLIENT} -q "DETACH TABLE t_url_nc"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE t_url_nc"
h1=$(hash)

${CLICKHOUSE_CLIENT} -q "DETACH TABLE t_url_nc"
${CLICKHOUSE_CLIENT} -q "ALTER NAMED COLLECTION ${NC} SET format = 'TSV'"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE t_url_nc"
h2=$(hash)

${CLICKHOUSE_CLIENT} -q "SELECT x FROM t_url_nc"
[ -n "$h1" ] && [ "$h1" != "\\N" ] && echo "hash is not NULL"
[ "$h1" != "$h2" ] && echo "hash changed with the format"

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE t_url_nc;
    DROP NAMED COLLECTION ${NC};
"
