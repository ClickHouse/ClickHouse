#!/usr/bin/env bash
# Tags: no-fasttest

# A `301` redirect from an S3 server at an endpoint of at most 10 characters (`http://abc`) used to throw `std::out_of_range`.
# The proxy below stands in for that server and redirects every request.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PORT_FILE=$(mktemp "${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}.XXXXXX.port")

python3 -c "
import threading
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

BODY = b'<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>PermanentRedirect</Code><Message>redirect</Message><Endpoint>def</Endpoint></Error>'
redirected = 0
lock = threading.Lock()

class Handler(BaseHTTPRequestHandler):
    def reply(self):
        global redirected
        if self.path == '/redirected':
            with lock:
                body = str(int(redirected > 0)).encode()
            self.send_response(200)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if self.path.startswith('http://def/'):
            with lock:
                redirected += 1
        self.send_response(301)
        self.send_header('Location', 'http://def/bbb/key.csv')
        self.send_header('x-amz-bucket-region', 'us-east-1')
        self.send_header('Content-Type', 'application/xml')
        self.send_header('Content-Length', str(len(BODY)))
        self.end_headers()
        if self.command != 'HEAD':
            self.wfile.write(BODY)

    do_GET = do_HEAD = reply

    def log_message(self, *args):
        pass

server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
with open('$PORT_FILE', 'w') as f:
    f.write(str(server.server_address[1]))
server.serve_forever()
" &
PROXY_PID=$!

trap 'kill $PROXY_PID 2>/dev/null; wait $PROXY_PID 2>/dev/null; rm -f "$PORT_FILE"' EXIT

for _ in {1..300}; do
    [ -s "$PORT_FILE" ] && break
    sleep 0.1
done
PROXY_PORT=$(cat "$PORT_FILE")
[ -n "$PROXY_PORT" ] || { echo 'the proxy did not start'; exit 1; }

http_proxy="http://127.0.0.1:${PROXY_PORT}" no_proxy="" ${CLICKHOUSE_LOCAL} --query "SELECT * FROM s3('http://abc/bbb/key.csv', NOSIGN, 'CSV', 'x String')" 2>&1 | grep -o -m1 'S3_ERROR'

# 1 when the client followed the redirect to `def`, which happens only after the endpoint check.
${CLICKHOUSE_CURL} "http://127.0.0.1:${PROXY_PORT}/redirected"
echo
