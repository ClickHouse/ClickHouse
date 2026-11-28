#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs a local HTTP server serving index pages

# A strong `ETag` identifies the content only within one resource. With failover options in the URL (`{a|b}`), the
# metadata and the data of an object are each read from the first reachable host, so after the first host fails, the
# second one can serve other bytes with the same `ETag` (and size). The page cache must not serve the bytes of the
# first host then.
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

read -r PORT_A PORT_B < <(python3 -c "
import socket
a = socket.socket(); a.bind(('127.0.0.1', 0))
b = socket.socket(); b.bind(('127.0.0.1', 0))
print(a.getsockname()[1], b.getsockname()[1])
a.close(); b.close()
")

# Both hosts serve `/d/f.csv` with the same strong `ETag` and size: host A serves `1`, host B serves `2`.
# After a request to `/fail` of host A, it answers every request with HTTP 503.
python3 -c "
import sys, threading
from http.server import HTTPServer, BaseHTTPRequestHandler

def make_handler(content, state):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def send_empty(self, status):
            self.send_response(status)
            self.send_header('Content-Length', '0')
            self.end_headers()

        def respond(self, with_body):
            path = self.path.split('?')[0]
            etag = None
            if path == '/fail':
                if with_body:
                    state['failed'] = True
                data, content_type = b'ok', 'text/plain'
            elif state['failed']:
                self.send_empty(503)
                return
            elif path == '/d/':
                data, content_type = b'<a href=\"f.csv\">f.csv</a>\n', 'text/html'
            elif path == '/d/f.csv':
                data, etag, content_type = content, '\"v1\"', 'text/csv'
            else:
                self.send_empty(404)
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
    return Handler

server_a = HTTPServer(('127.0.0.1', int(sys.argv[1])), make_handler(b'1\n', {'failed': False}))
server_b = HTTPServer(('127.0.0.1', int(sys.argv[2])), make_handler(b'2\n', {'failed': False}))
threading.Thread(target=server_b.serve_forever, daemon=True).start()
server_a.serve_forever()
" "${PORT_A}" "${PORT_B}" &
HTTP_PID=$!
trap "kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null" EXIT

for port in "${PORT_A}" "${PORT_B}"; do
    for _ in $(seq 1 50); do
        curl -s "http://127.0.0.1:${port}/d/" -o /dev/null 2>/dev/null && break
        sleep 0.1
    done
done

url="http://127.0.0.1:{${PORT_A}|${PORT_B}}/d/*.csv"
settings="use_page_cache_for_object_storage = 1, allow_url_wildcard_from_index_pages = 1, http_max_tries = 1"

${CLICKHOUSE_LOCAL} --path "${work_dir}/db" --config-file "${work_dir}/config.yaml" --query "
    SELECT 'before', x FROM url('${url}', CSV, 'x UInt64') SETTINGS ${settings};
    SELECT * FROM url('http://127.0.0.1:${PORT_A}/fail', RawBLOB);
    SELECT 'after', x FROM url('${url}', CSV, 'x UInt64') SETTINGS ${settings};"

rm -rf "${work_dir}"
