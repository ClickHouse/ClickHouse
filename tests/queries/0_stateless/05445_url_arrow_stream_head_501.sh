#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test build has no Arrow support
# `ArrowStream` from `url` when the server streams the body without `Content-Length` and answers HEAD with 501: the
# seekability probe used to rethrow the failed HEAD request, so every record batch failed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

data_file="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.arrowstream"

${CLICKHOUSE_LOCAL} --query "
    SELECT number AS x, number * 10 AS y FROM numbers(10)
    SETTINGS max_block_size = 4, output_format_arrow_compression_method = 'none'
    FORMAT ArrowStream" > "$data_file"

HTTP_PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

# `BaseHTTPRequestHandler` without `do_HEAD` answers HEAD with 501.
python3 -c "
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
data = open('$data_file', 'rb').read()

class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    def do_GET(self):
        self.send_response(200)
        self.send_header('Transfer-Encoding', 'chunked')
        self.end_headers()
        self.wfile.write(b'%x\r\n' % len(data) + data + b'\r\n0\r\n\r\n')
    def log_message(self, *args):
        pass

ThreadingHTTPServer(('127.0.0.1', $HTTP_PORT), Handler).serve_forever()
" &
HTTP_PID=$!
trap 'kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null; rm -f "$data_file"' EXIT

for _ in $(seq 1 50); do
    curl -s "http://127.0.0.1:$HTTP_PORT/" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

url="url('http://127.0.0.1:$HTTP_PORT/data', ArrowStream, 'x UInt64, y UInt64')"
${CLICKHOUSE_LOCAL} --query "SELECT count() FROM $url SETTINGS http_make_head_request = 1"
${CLICKHOUSE_LOCAL} --query "SELECT sum(x), sum(y) FROM $url SETTINGS http_make_head_request = 1"
${CLICKHOUSE_LOCAL} --query "SELECT sum(y) FROM $url SETTINGS http_make_head_request = 1"
