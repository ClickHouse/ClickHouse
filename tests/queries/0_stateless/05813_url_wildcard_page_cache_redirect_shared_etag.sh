#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs a local HTTP server serving index pages

# The `ETag` of a redirected URL is of the resource that the redirect leads to, and the redirect can switch to another
# resource with the same `ETag` (and size) while the URL of the object stays the same. The page cache must not serve
# the bytes of the first resource after such a switch.
#
# The CI server configures no page cache, so everything runs in a private clickhouse-local that has one.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

work_dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_page_cache"
rm -rf "${work_dir}"
mkdir -p "${work_dir}"

> "${work_dir}/config.yaml" echo "
page_cache_max_size: 134217728
"

HTTP_PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

# `/d/f.csv` redirects to `/versions/a.csv` (`1`), and after a request to `/switch`, to `/versions/b.csv` (`2`).
# Both have the same strong `ETag` and size.
python3 -c "
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler

FILES = {'/versions/a.csv': b'1\n', '/versions/b.csv': b'2\n'}
state = {'target': '/versions/a.csv'}

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def respond(self, with_body):
        path = self.path.split('?')[0]
        etag = None
        if path == '/switch':
            if with_body:
                state['target'] = '/versions/b.csv'
            data, content_type = b'ok', 'text/plain'
        elif path == '/d/':
            data, content_type = b'<a href=\"f.csv\">f.csv</a>\n', 'text/html'
        elif path == '/d/f.csv':
            self.send_response(302)
            self.send_header('Location', state['target'])
            self.send_header('Content-Length', '0')
            self.end_headers()
            return
        elif path in FILES:
            data, etag, content_type = FILES[path], '\"v1\"', 'text/csv'
        else:
            self.send_response(404)
            self.send_header('Content-Length', '0')
            self.end_headers()
            return
        rng = self.headers.get('Range')
        status = 200
        if rng and etag is not None:
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
        if etag is not None:
            self.send_header('ETag', etag)
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

HTTPServer(('127.0.0.1', int(sys.argv[1])), Handler).serve_forever()
" "${HTTP_PORT}" &
HTTP_PID=$!
trap "kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null" EXIT

for _ in $(seq 1 50); do
    curl -s "http://127.0.0.1:$HTTP_PORT/d/" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

base="http://127.0.0.1:${HTTP_PORT}"
settings="use_page_cache_for_object_storage = 1, allow_url_wildcard_from_index_pages = 1, max_http_get_redirects = 10, http_max_tries = 1"

${CLICKHOUSE_LOCAL} --path "${work_dir}/db" --config-file "${work_dir}/config.yaml" --query "
    SELECT 'before', x FROM url('${base}/d/*.csv', CSV, 'x UInt64') SETTINGS ${settings};
    SELECT * FROM url('${base}/switch', RawBLOB);
    SELECT 'after', x FROM url('${base}/d/*.csv', CSV, 'x UInt64') SETTINGS ${settings};"

rm -rf "${work_dir}"
