#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: depends on delta-kernel-rs
# Tag no-msan: delta-kernel-rs is not built with MSan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PORT_FILE=$(mktemp "./${CLICKHOUSE_DATABASE}.XXXXXX.port")

python3 -u -c "
import threading
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

hits = 0

class Forbidden(BaseHTTPRequestHandler):
    def do_GET(self):
        global hits
        if self.path == '/hits':
            body = str(hits).encode()
            self.send_response(200)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        hits += 1
        self.send_error(500)

    do_HEAD = do_GET

    def log_message(self, *args):
        pass

forbidden = ThreadingHTTPServer(('127.0.0.1', 0), Forbidden)
forbidden_port = forbidden.server_address[1]

class Allowed(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(302)
        self.send_header('Location', 'http://127.0.0.1:%d/redirected' % forbidden_port)
        self.send_header('Content-Length', '0')
        self.end_headers()

    do_HEAD = do_GET

    def log_message(self, *args):
        pass

allowed = ThreadingHTTPServer(('127.0.0.1', 0), Allowed)
threading.Thread(target=forbidden.serve_forever, daemon=True).start()
with open('$PORT_FILE', 'w') as f:
    f.write('%d %d' % (allowed.server_address[1], forbidden_port))
allowed.serve_forever()
" &
HTTP_PID=$!
trap 'kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null; rm -f "$PORT_FILE"' EXIT

for _ in {1..300}; do
    [[ -s "$PORT_FILE" ]] && break
    sleep 0.1
done
read -r ALLOWED_PORT FORBIDDEN_PORT < "$PORT_FILE"

$CLICKHOUSE_LOCAL --query "SELECT * FROM deltaLake('http://127.0.0.1:$ALLOWED_PORT/bucket/table/', NOSIGN) SETTINGS allow_experimental_delta_kernel_rs = 1" > /dev/null 2>&1

${CLICKHOUSE_CURL} -sS "http://127.0.0.1:$FORBIDDEN_PORT/hits"
echo
