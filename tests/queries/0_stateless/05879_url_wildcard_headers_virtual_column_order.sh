#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs a local HTTP server serving index pages.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `url` with a listable glob in the path is read by the object storage over HTTP index pages. Its
# source added the `_headers` virtual column to its chunks after all the other virtual columns,
# while the header of the source lists the virtual columns in the order they are requested in. With
# `_headers` requested before another virtual column, the query read one column in place of the
# other: an exception about the type of a column at best, a server crash otherwise.

HTTP_PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

# An index page listing one part, and a response header to look for in `_headers`.
python3 -c "
from http.server import HTTPServer, BaseHTTPRequestHandler

PARTS = {'/data/part1.tsv': b'hello\nworld\n'}
INDEX = b'<a href=\"part1.tsv\">part1.tsv</a>\n'

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def body(self):
        path = self.path.split('?')[0]
        if path == '/data/':
            return INDEX, 'text/html'
        return PARTS.get(path), 'text/plain'

    def respond(self, with_body):
        data, content_type = self.body()
        if data is None:
            self.send_response(404)
            self.end_headers()
            if with_body:
                self.wfile.write(b'Not Found')
            return
        # The part can be read with a range request, depending on the settings.
        rng = self.headers.get('Range')
        status = 200
        if rng:
            begin, end = rng.split('=')[1].split('-')
            begin = int(begin)
            end = min(int(end), len(data) - 1) if end else len(data) - 1
            content_range = 'bytes %d-%d/%d' % (begin, end, len(data))
            data = data[begin:end + 1]
            status = 206
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Accept-Ranges', 'bytes')
        if status == 206:
            self.send_header('Content-Range', content_range)
        self.send_header('Last-Modified', 'Thu, 01 Jan 2026 00:00:00 GMT')
        self.send_header('X-Test', 'hello')
        self.end_headers()
        if with_body:
            try:
                self.wfile.write(data)
            except (BrokenPipeError, ConnectionResetError):
                pass

    def do_HEAD(self):
        self.respond(False)

    def do_GET(self):
        self.respond(True)

HTTPServer(('127.0.0.1', $HTTP_PORT), Handler).serve_forever()
" &
HTTP_PID=$!
trap "kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null" EXIT

for _ in $(seq 1 50); do
    curl -s "http://127.0.0.1:$HTTP_PORT/data/" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

GLOB_URL="http://127.0.0.1:${HTTP_PORT}/data/*.tsv"

$CLICKHOUSE_CLIENT -q "
SET allow_url_wildcard_from_index_pages = 1;
SET http_max_tries = 1;

SELECT '-- _headers before another virtual column';
SELECT _headers['X-Test'], _size, _file, s
FROM url('${GLOB_URL}', LineAsString, 's String') ORDER BY s;

SELECT '-- _headers between other virtual columns';
SELECT _file, _headers['X-Test'], _size, s
FROM url('${GLOB_URL}', LineAsString, 's String') ORDER BY s;

SELECT '-- _headers after the other virtual columns';
SELECT _file, _size, _headers['X-Test'], s
FROM url('${GLOB_URL}', LineAsString, 's String') ORDER BY s;

SELECT '-- _headers in SELECT, another virtual column in WHERE';
SELECT _headers['X-Test'], s
FROM url('${GLOB_URL}', LineAsString, 's String') WHERE _size = 12 ORDER BY s;

SELECT '-- the query of the AST fuzzer, with the condition negated to get rows';
SELECT empty(_headers['X-Long']), s
FROM url('${GLOB_URL}', LineAsString, 's String') WHERE NOT isNotDistinctFrom(_size, true) ORDER BY s;

-- A read of only virtual columns can take the number of rows from the cache, without a request for
-- the data, but not if \`_headers\` is requested: it is the headers of the response to that request.
SELECT '-- the number of rows is in the cache';
SELECT count()
FROM url('${GLOB_URL}', LineAsString, 's String');

SELECT '-- only virtual columns, _headers before other virtual columns';
SELECT _headers['X-Test'], _size, _file
FROM url('${GLOB_URL}', LineAsString, 's String');

SELECT '-- only virtual columns, _headers after other virtual columns';
SELECT _file, _size, _headers['X-Test']
FROM url('${GLOB_URL}', LineAsString, 's String');
"
