#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: starts a local HTTP server with python3.
# no-replicated-database: `DETACH` / `ATTACH` of the table.

# `system.tables.modification_hash` of a `URL` table must cover whether a declared column is read from the
# file or from the Hive partition in the path. That is captured from `use_hive_partitioning` when the table
# is loaded, so a `DETACH` / `ATTACH` with another value changes the rows read from the same resource with
# the same strong `ETag`, and the hash must change.

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
        if self.path == '/p=0/data.jsonl':
            code, headers, body = 200, {'ETag': '\"stable\"'}, b'{\"id\":1,\"p\":1}\n'
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
    curl -s "http://127.0.0.1:$HTTP_PORT/p=0/data.jsonl" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

${CLICKHOUSE_CLIENT} --use_hive_partitioning 0 -q "
    DROP TABLE IF EXISTS t_url_hive;
    CREATE TABLE t_url_hive (id UInt64, p UInt64) ENGINE = URL('http://127.0.0.1:$HTTP_PORT/p=0/data.jsonl', 'JSONEachRow');
"

function hash()
{
    ${CLICKHOUSE_CLIENT} -q "SELECT modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 't_url_hive'"
}

function reattach()
{
    ${CLICKHOUSE_CLIENT} -q "DETACH TABLE t_url_hive"
    ${CLICKHOUSE_CLIENT} --use_hive_partitioning "$1" -q "ATTACH TABLE t_url_hive"
}

reattach 0
${CLICKHOUSE_CLIENT} -q "SELECT p FROM t_url_hive"
h1=$(hash)

reattach 0
h2=$(hash)

reattach 1
${CLICKHOUSE_CLIENT} -q "SELECT p FROM t_url_hive"
h3=$(hash)

[ -n "$h1" ] && [ "$h1" != "\\N" ] && echo "hash is not NULL"
[ "$h1" = "$h2" ] && echo "hash is the same after a reattachment with the same configuration"
[ "$h1" != "$h3" ] && echo "hash changed with the Hive partitioning"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_url_hive"
