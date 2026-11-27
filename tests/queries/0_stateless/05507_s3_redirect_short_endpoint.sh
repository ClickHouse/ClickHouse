#!/usr/bin/env bash
# Tags: no-fasttest

# A `301` redirect from an S3 server at an endpoint of at most 10 characters (`http://abc`) used to throw `std::out_of_range`.
# The proxy below stands in for that server and redirects every request.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PROXY_PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

python3 -c "
import threading
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

BODY = b'<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>PermanentRedirect</Code><Message>redirect</Message><Endpoint>def</Endpoint></Error>'
redirected = 0
lock = threading.Lock()

class Handler(BaseHTTPRequestHandler):
    def reply(self):
        global redirected
        if self.path in ('/ping', '/redirected'):
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

ThreadingHTTPServer(('127.0.0.1', $PROXY_PORT), Handler).serve_forever()
" &
PROXY_PID=$!

trap 'kill $PROXY_PID 2>/dev/null' EXIT

for _ in {1..100}; do
    if [ "$(${CLICKHOUSE_CURL} -o /dev/null -w "%{http_code}" "http://127.0.0.1:${PROXY_PORT}/ping")" = "200" ]; then
        break
    fi
    sleep 0.1
done

http_proxy="http://127.0.0.1:${PROXY_PORT}" no_proxy="" ${CLICKHOUSE_LOCAL} --query "SELECT * FROM s3('http://abc/bbb/key.csv', NOSIGN, 'CSV', 'x String')" 2>&1 | grep -o -m1 'S3_ERROR'

# 1 when the client followed the redirect to `def`, which happens only after the endpoint check.
${CLICKHOUSE_CURL} "http://127.0.0.1:${PROXY_PORT}/redirected"
echo
