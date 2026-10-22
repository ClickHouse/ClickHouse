#!/usr/bin/env bash
# Tags: no-fasttest

# A status above the largest named `HTTPStatus` (511) must be reported as received.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PORT_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.port"
rm -f "$PORT_FILE"

python3 -c "
import os
import sys
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

class Handler(BaseHTTPRequestHandler):
    def do_HEAD(self):
        self.send_response(int(self.path.strip('/')), 'Unknown Error')
        self.send_header('Content-Type', 'text/plain')
        self.send_header('Content-Length', '3')
        self.end_headers()

    def do_GET(self):
        self.do_HEAD()
        self.wfile.write(b'ok\n')

    def log_message(self, *args):
        pass

server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
port_file = sys.argv[1]
with open(port_file + '.tmp', 'w') as f:
    f.write(str(server.server_address[1]))
os.rename(port_file + '.tmp', port_file)
server.serve_forever()
" "$PORT_FILE" &
HTTP_PID=$!

trap 'kill $HTTP_PID 2>/dev/null; rm -f "$PORT_FILE"' EXIT

for _ in {1..100}; do
    [ -s "$PORT_FILE" ] && break
    sleep 0.1
done
PORT=$(cat "$PORT_FILE")

for status in 512 520 999; do
    ${CLICKHOUSE_CLIENT} --query "SELECT * FROM url('http://127.0.0.1:${PORT}/${status}', TSV, 'c String') SETTINGS http_max_tries = 1" 2>&1 \
        | grep -o -m1 "HTTP status code: [0-9]* '[^']*'"
done
