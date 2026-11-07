#!/usr/bin/env bash
# Tags: no-fasttest
# Positional reads of `url()` when a range response carries only a part of the range, no data, or a wrong range.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.parquet")
PORT_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.port")

# Smaller than 128 KiB, so that the reader fetches the whole file with one positional read at offset 0.
$CLICKHOUSE_LOCAL --query "SELECT number AS x FROM numbers(10000) FORMAT Parquet" > "$DATA_FILE"

python3 -u -c "
import sys
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

with open(sys.argv[1], 'rb') as f:
    DATA = f.read()
SIZE = len(DATA)
PIECE = SIZE // 5 + 1

class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def reply(self, status, body, content_range=None):
        self.send_response(status)
        self.send_header('Content-Type', 'application/octet-stream')
        self.send_header('Accept-Ranges', 'bytes')
        if content_range is not None:
            self.send_header('Content-Range', content_range)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_HEAD(self):
        self.send_response(200)
        self.send_header('Accept-Ranges', 'bytes')
        self.send_header('Content-Length', str(SIZE))
        self.end_headers()

    def do_GET(self):
        header = self.headers.get('Range')
        if header is None:
            return self.reply(200, DATA)
        begin, end = header.split('=')[1].split('-')
        begin, end = int(begin), min(int(end), SIZE - 1)
        if self.path == '/pieces':
            # At most PIECE bytes of every requested range.
            end = min(end, begin + PIECE - 1)
            return self.reply(206, DATA[begin:end + 1], 'bytes {}-{}/{}'.format(begin, end, SIZE))
        if self.path == '/empty':
            return self.reply(206, b'', 'bytes {}-{}/{}'.format(begin, end, SIZE))
        if self.path == '/shrunk':
            # The whole file, which got shorter after the HEAD request.
            return self.reply(200, DATA[:SIZE // 2])
        if self.path == '/misaligned':
            # The beginning of the file, whatever range was requested.
            return self.reply(206, DATA[:PIECE], 'bytes 0-{}/{}'.format(PIECE - 1, SIZE))
        if self.path == '/reflected':
            # An unparseable range that reflects the request's credentials.
            return self.reply(206, DATA[:PIECE], self.headers.get('Authorization', ''))
        self.send_error(404)

    def log_message(self, *args):
        pass

class Server(ThreadingHTTPServer):
    # The client drops the connection when it rejects a response before reading its body.
    def handle_error(self, request, client_address):
        if not isinstance(sys.exc_info()[1], ConnectionError):
            super().handle_error(request, client_address)

server = Server(('127.0.0.1', 0), Handler)
with open(sys.argv[2], 'w') as f:
    f.write(str(server.server_address[1]))
server.serve_forever()
" "$DATA_FILE" "$PORT_FILE" &
HTTP_PID=$!
trap 'kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null; rm -f "$PORT_FILE" "$DATA_FILE"' EXIT

for _ in {1..300}; do
    [[ -s "$PORT_FILE" ]] && break
    sleep 0.1
done
URL="http://127.0.0.1:$(cat "$PORT_FILE")"

SETTINGS="input_format_allow_seeks = 1, http_make_head_request = 1, parallel_replicas_for_cluster_engines = 0, http_retry_initial_backoff_ms = 1, http_retry_max_backoff_ms = 10"

$CLICKHOUSE_CLIENT --query "SELECT count(), sum(x) FROM url('$URL/pieces', Parquet, 'x UInt64') SETTINGS $SETTINGS, http_max_tries = 1"

$CLICKHOUSE_CLIENT --query "SELECT count(), sum(x) FROM url('$URL/empty', Parquet, 'x UInt64') SETTINGS $SETTINGS, http_max_tries = 2" 2>&1 \
    | grep -q -F '(UNEXPECTED_END_OF_FILE)' && echo "empty: UNEXPECTED_END_OF_FILE"

$CLICKHOUSE_CLIENT --query "SELECT count(), sum(x) FROM url('$URL/shrunk', Parquet, 'x UInt64') SETTINGS $SETTINGS, http_max_tries = 2" 2>&1 \
    | grep -q -F '(UNEXPECTED_END_OF_FILE)' && echo "shrunk: UNEXPECTED_END_OF_FILE"

$CLICKHOUSE_CLIENT --query "SELECT count(), sum(x) FROM url('$URL/misaligned', Parquet, 'x UInt64') SETTINGS $SETTINGS, http_max_tries = 2" 2>&1 \
    | grep -q -F '(HTTP_RANGE_NOT_SATISFIABLE)' && echo "misaligned: HTTP_RANGE_NOT_SATISFIABLE"

# Only the error line: the client also echoes the submitted query, which carries the token as typed.
TOKEN="tok${CLICKHOUSE_TEST_UNIQUE_NAME}"
ERROR=$($CLICKHOUSE_CLIENT --query "SELECT count(), sum(x) FROM url('$URL/reflected', Parquet, 'x UInt64', headers('Authorization' = 'Bearer $TOKEN'))
    SETTINGS $SETTINGS, http_max_tries = 2" 2>&1 | grep -F 'The server returned the range')
grep -q -F '(HTTP_RANGE_NOT_SATISFIABLE)' <<< "$ERROR" && grep -q -F "range 'Bearer [HIDDEN]'" <<< "$ERROR" \
    && ! grep -q -F "$TOKEN" <<< "$ERROR" && echo "reflected: HTTP_RANGE_NOT_SATISFIABLE, token scrubbed"

$CLICKHOUSE_CLIENT --query "SELECT 1"
