#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs a local HTTP server serving index pages.

# A wildcard in a directory with a space is expanded from the HTTP index page of that directory,
# whether the space is written literally or as `%20`.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

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

FILES = {'/some%20dir/a%20b.csv': b'1\n', '/some%20dir/c%20d.csv': b'2\n', '/some%20dir/e%20f.tsv': b'3\n'}
INDEX = b'<a href=\"a%20b.csv\">a b.csv</a>\n<a href=\"c%20d.csv\">c d.csv</a>\n<a href=\"e%20f.tsv\">e f.tsv</a>\n'

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
trap "kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null" EXIT

for _ in $(seq 1 50); do
    curl -s "http://127.0.0.1:$HTTP_PORT/some%20dir/" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

SETTINGS="SETTINGS allow_url_wildcard_from_index_pages = 1, use_hive_partitioning = 0, http_max_tries = 1"

${CLICKHOUSE_LOCAL} --query "SELECT x FROM url('http://127.0.0.1:${HTTP_PORT}/some dir/*.csv', 'CSV', 'x UInt32') ORDER BY x ${SETTINGS}"
${CLICKHOUSE_LOCAL} --query "SELECT x FROM url('http://127.0.0.1:${HTTP_PORT}/some%20dir/*.csv', 'CSV', 'x UInt32') ORDER BY x ${SETTINGS}"
${CLICKHOUSE_LOCAL} --query "SELECT x FROM url('http://127.0.0.1:${HTTP_PORT}/some dir/{a b,e f}.*', 'CSV', 'x UInt32') ORDER BY x ${SETTINGS}"
