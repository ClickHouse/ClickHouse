#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs a local HTTP server.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A `POST` read has no `HEAD` to ask, so `_size` and `_time` must come from the `POST` response itself.

HTTP_PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

python3 -c "
from http.server import HTTPServer, BaseHTTPRequestHandler

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        self.send_response(405)
        self.end_headers()

    def do_HEAD(self):
        self.do_GET()

    def do_POST(self):
        data = b'1\n'
        self.send_response(200)
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Last-Modified', 'Wed, 01 Jan 2025 00:00:00 GMT')
        self.end_headers()
        self.wfile.write(data)

HTTPServer(('127.0.0.1', $HTTP_PORT), Handler).serve_forever()
" &
HTTP_PID=$!
trap "kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null" EXIT

for _ in $(seq 1 50); do
    curl -s -X POST "http://127.0.0.1:$HTTP_PORT/" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

URL="http://127.0.0.1:${HTTP_PORT}/data.tsv"
for skip in 0 1; do
    $CLICKHOUSE_LOCAL --query "SET engine_url_skip_empty_files = ${skip}, http_max_tries = 1, session_timezone = 'UTC';
        SELECT x, _size, _time FROM url('${URL}', TSV, 'x UInt8', http_method = 'POST');
        SELECT count() FROM url('${URL}', TSV, 'x UInt8', http_method = 'POST') WHERE _time = toDateTime('2025-01-01 00:00:00');"
done
