#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs a local HTTP server serving index pages.

# A `URL` table over a named collection whose locked `url` has spaces can be created and attached again:
# the spaces encoded at runtime are not persisted as a `url` override, which the collection forbids.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

nc="${CLICKHOUSE_TEST_UNIQUE_NAME}_file"
wildcard_nc="${CLICKHOUSE_TEST_UNIQUE_NAME}_wildcard"

HTTP_PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

# Index pages link to entries with spaces percent-encoded, as web servers list them.
python3 -c "
from http.server import HTTPServer, BaseHTTPRequestHandler

FILES = {'/some%20dir/a%20b.csv': b'1\n', '/some%20dir/c%20d.csv': b'2\n'}
INDEX = b'<a href=\"a%20b.csv\">a b.csv</a>\n<a href=\"c%20d.csv\">c d.csv</a>\n'

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def body(self):
        path = self.path.split('?')[0]
        if path == '/some%20dir/':
            return INDEX, 'text/html'
        return FILES.get(path), 'text/plain'

    def respond(self, with_body):
        data, content_type = self.body()
        if data is None:
            self.send_response(404)
            self.end_headers()
            if with_body:
                self.wfile.write(b'Not Found')
            return
        self.send_response(200)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        if with_body:
            self.wfile.write(data)

    def do_HEAD(self):
        self.respond(False)

    def do_GET(self):
        self.respond(True)

HTTPServer(('127.0.0.1', $HTTP_PORT), Handler).serve_forever()
" &
HTTP_PID=$!

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_file"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_wildcard"
    ${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION IF EXISTS $nc"
    ${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION IF EXISTS $wildcard_nc"
    kill $HTTP_PID 2>/dev/null
    wait $HTTP_PID 2>/dev/null
}
trap cleanup EXIT

for _ in $(seq 1 50); do
    curl -s "http://127.0.0.1:$HTTP_PORT/some%20dir/" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

${CLICKHOUSE_CLIENT} --query "CREATE NAMED COLLECTION $nc AS url = 'http://127.0.0.1:$HTTP_PORT/some dir/a b.csv' NOT OVERRIDABLE, format = 'CSV'"
${CLICKHOUSE_CLIENT} --query "CREATE NAMED COLLECTION $wildcard_nc AS url = 'http://127.0.0.1:$HTTP_PORT/some dir/*.csv' NOT OVERRIDABLE, format = 'CSV'"

echo 'file'
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_file (x UInt32) ENGINE = URL($nc)"
${CLICKHOUSE_CLIENT} --query "SELECT x FROM t_file"
${CLICKHOUSE_CLIENT} --query "SELECT countSubstrings(engine_full, 'some') FROM system.tables WHERE database = currentDatabase() AND name = 't_file'"
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_file"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_file"
${CLICKHOUSE_CLIENT} --query "SELECT x FROM t_file"

echo 'wildcard'
${CLICKHOUSE_CLIENT} --allow_url_wildcard_from_index_pages 1 --query "CREATE TABLE t_wildcard (x UInt32) ENGINE = URL($wildcard_nc)"
${CLICKHOUSE_CLIENT} --query "SELECT x FROM t_wildcard ORDER BY x SETTINGS use_hive_partitioning = 0"
${CLICKHOUSE_CLIENT} --query "SELECT countSubstrings(engine_full, 'some') FROM system.tables WHERE database = currentDatabase() AND name = 't_wildcard'"
${CLICKHOUSE_CLIENT} --query "DETACH TABLE t_wildcard"
${CLICKHOUSE_CLIENT} --query "ATTACH TABLE t_wildcard"
${CLICKHOUSE_CLIENT} --query "SELECT x FROM t_wildcard ORDER BY x SETTINGS use_hive_partitioning = 0"
