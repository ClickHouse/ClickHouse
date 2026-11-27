#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: reads a Parquet file.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PORT_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.port")
PARQUET_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.parquet")
$CLICKHOUSE_CLIENT -q "SELECT number FROM numbers(3) FORMAT Parquet" > "$PARQUET_FILE"

# Supports range requests and sends `X-Test: get` with a GET, `X-Test: head` with a HEAD.
python3 -u -c "
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

files = {'/a.tsv': b'hello\\nworld\\n', '/b.parquet': open('$PARQUET_FILE', 'rb').read()}

class Handler(BaseHTTPRequestHandler):
    def respond(self, with_body):
        data = files.get(self.path)
        if data is None:
            self.send_error(404)
            return
        status = 200
        rng = self.headers.get('Range')
        if rng:
            begin, end = rng.split('=')[1].split('-')
            begin = int(begin)
            end = min(int(end), len(data) - 1) if end else len(data) - 1
            content_range = 'bytes %d-%d/%d' % (begin, end, len(data))
            data = data[begin:end + 1]
            status = 206
        self.send_response(status)
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Accept-Ranges', 'bytes')
        if status == 206:
            self.send_header('Content-Range', content_range)
        self.send_header('X-Test', 'get' if with_body else 'head')
        self.end_headers()
        if with_body:
            self.wfile.write(data)

    def do_HEAD(self):
        self.respond(False)

    def do_GET(self):
        self.respond(True)

    def log_message(self, *args):
        pass

server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
with open('$PORT_FILE', 'w') as f:
    f.write(str(server.server_address[1]))
server.serve_forever()
" &
HTTP_PID=$!
trap 'kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null; rm -f "$PORT_FILE" "$PARQUET_FILE"' EXIT

for _ in {1..300}; do
    [[ -s "$PORT_FILE" ]] && break
    sleep 0.1
done
URL="http://127.0.0.1:$(cat "$PORT_FILE")"

# A sequential read: the headers of the GET.
$CLICKHOUSE_CLIENT -q "SELECT s, _headers['X-Test'] FROM url('$URL/a.tsv', LineAsString, 's String') ORDER BY s SETTINGS max_download_threads = 1"

# Only range requests (parallel download): the headers of one of the range responses.
# `urlCluster` divides `max_download_threads` between its streams, so it would read the file with one GET.
$CLICKHOUSE_CLIENT -q "SELECT s, _headers['X-Test'], _headers['Content-Range'] != '' FROM url('$URL/a.tsv', LineAsString, 's String') ORDER BY s SETTINGS max_download_threads = 2, max_download_buffer_size = 4, parallel_replicas_for_cluster_engines = 0"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM url('$URL/a.tsv', LineAsString, 's String') WHERE _headers['X-Test'] = 'get' SETTINGS max_download_threads = 2, max_download_buffer_size = 4, parallel_replicas_for_cluster_engines = 0"

# The Parquet reader reads with range requests as well.
$CLICKHOUSE_CLIENT -q "SELECT number, _headers['X-Test'], _headers['Content-Range'] != '' FROM url('$URL/b.parquet', Parquet, 'number UInt64') ORDER BY number SETTINGS parallel_replicas_for_cluster_engines = 0"
