#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs a local HTTP server serving index pages

# With `enable_url_encoding = 1`, an encoded slash `%2F` in the path of a URL is decoded into a path separator, so the
# same listed path `d/a%2Fb.csv` names `/d/a%2Fb.csv` without it and `/d/a/b.csv` with it. These are different resources
# with the same size and the same strong `ETag`, so the setting must be a part of the key of the page cache: each read
# must see its own resource, in both orders of the queries.
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

python3 -c "
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler

port = int(sys.argv[1])

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def respond(self, with_body):
        path = self.path.split('?')[0]
        etag = None
        if path == '/d/':
            data, content_type = ('<a href=\"http://127.0.0.1:%d/d/a%%2Fb.csv\">a%%2Fb.csv</a>\n' % port).encode(), 'text/html'
        elif path == '/d/a%2Fb.csv':
            data, content_type, etag = b'1\n', 'text/csv', '\"v1\"'
        elif path == '/d/a/b.csv':
            data, content_type, etag = b'2\n', 'text/csv', '\"v1\"'
        else:
            self.send_response(404)
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

HTTPServer(('127.0.0.1', port), Handler).serve_forever()
" "${HTTP_PORT}" &
HTTP_PID=$!
trap "kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null" EXIT

for _ in $(seq 1 50); do
    curl -s "http://127.0.0.1:$HTTP_PORT/d/" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

base="http://127.0.0.1:${HTTP_PORT}"
settings="use_page_cache_for_object_storage = 1, allow_url_wildcard_from_index_pages = 1, http_max_tries = 1"

for order in "0 1" "1 0"; do
    rm -rf "${work_dir}/db"
    query=""
    for encoding in $order; do
        query+="SELECT 'enable_url_encoding = ${encoding}', x FROM url('${base}/d/*.csv', CSV, 'x UInt64') SETTINGS ${settings}, enable_url_encoding = ${encoding};"
    done
    ${CLICKHOUSE_LOCAL} --path "${work_dir}/db" --config-file "${work_dir}/config.yaml" --query "${query}"
done

rm -rf "${work_dir}"
