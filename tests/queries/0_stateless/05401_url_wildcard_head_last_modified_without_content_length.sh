#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs a local HTTP server serving index pages.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A `GET` read through index-page wildcards takes `_time` from a `HEAD` request. A `HEAD` answer
# without `Content-Length` is valid, and its `Last-Modified` must still reach `_time`.

HTTP_PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

python3 -c "
from http.server import HTTPServer, BaseHTTPRequestHandler

PARTS = {'/data/part1.tsv': b'1\n', '/data/part2.tsv': b'2\n'}
INDEX = b'<a href=\"part1.tsv\">part1.tsv</a>\n<a href=\"part2.tsv\">part2.tsv</a>\n'
MODIFIED = 'Wed, 01 Jan 2025 00:00:00 GMT'

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_HEAD(self):
        path = self.path.split('?')[0]
        if path not in PARTS:
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header('Last-Modified', MODIFIED)
        self.end_headers()

    def do_GET(self):
        path = self.path.split('?')[0]
        data = INDEX if path == '/data/' else PARTS.get(path)
        if data is None:
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Last-Modified', MODIFIED)
        self.end_headers()
        self.wfile.write(data)

HTTPServer(('127.0.0.1', $HTTP_PORT), Handler).serve_forever()
" &
HTTP_PID=$!
trap "kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null" EXIT

for _ in $(seq 1 50); do
    curl -s "http://127.0.0.1:$HTTP_PORT/data/" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

GLOB_URL="http://127.0.0.1:${HTTP_PORT}/data/*.tsv"
SETTINGS="SET allow_url_wildcard_from_index_pages = 1, use_hive_partitioning = 0, http_max_tries = 1, session_timezone = 'UTC';"

$CLICKHOUSE_LOCAL --query "${SETTINGS}
    SELECT _file, x, _time FROM url('${GLOB_URL}', TSV, 'x UInt8') ORDER BY _file;
    SELECT count() FROM url('${GLOB_URL}', TSV, 'x UInt8') WHERE _time = toDateTime('2025-01-01 00:00:00');"
