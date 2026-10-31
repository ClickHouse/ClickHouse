#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: `Lance` requires the Rust build.
# Tag no-msan: `Lance` is disabled in MSan builds, because the `ring` crate does not build with MSan.

# The `Lance` object store is a separate HTTP client, which cannot check redirect destinations against
# the `remote_host_filter`, so it must not follow redirects at all: an allowed endpoint must not make
# the server contact another host.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

free_port()
{
    python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
"
}

REDIRECTING_PORT=$(free_port)
TARGET_PORT=$(free_port)
TARGET_LOG="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_target.log"
: > "${TARGET_LOG}"

# The first server redirects every request to the second one, which records the requests it gets.
run_server()
{
    python3 -c "
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler

port, target, log = int(sys.argv[1]), sys.argv[2], sys.argv[3]

class Handler(BaseHTTPRequestHandler):
    def respond(self):
        if target:
            self.send_response(307)
            self.send_header('Location', target + self.path)
        else:
            with open(log, 'a') as f:
                f.write(f\"{self.headers.get('User-Agent')}\n\")
            self.send_response(404)
        self.send_header('Content-Length', '0')
        self.end_headers()
    do_GET = do_HEAD = respond
    def log_message(self, *args):
        pass

HTTPServer(('127.0.0.1', port), Handler).serve_forever()
" "$@" &
}

run_server "${REDIRECTING_PORT}" "http://127.0.0.1:${TARGET_PORT}" ""
REDIRECTING_PID=$!
run_server "${TARGET_PORT}" "" "${TARGET_LOG}"
TARGET_PID=$!
trap 'kill ${REDIRECTING_PID} ${TARGET_PID} 2>/dev/null; wait ${REDIRECTING_PID} ${TARGET_PID} 2>/dev/null; rm -f "${TARGET_LOG}"' EXIT

for port in "${REDIRECTING_PORT}" "${TARGET_PORT}"; do
    for _ in $(seq 1 100); do
        curl -s "http://127.0.0.1:${port}/" -o /dev/null 2>/dev/null && break
        sleep 0.1
    done
done
: > "${TARGET_LOG}"

${CLICKHOUSE_CLIENT} --allow_experimental_lance=1 --query "
    SELECT * FROM lanceS3('http://127.0.0.1:${REDIRECTING_PORT}/bucket/dataset.lance', NOSIGN)
" 2>&1 | grep -c -m1 '307 Temporary Redirect'

# The S3 client of ClickHouse may follow the redirect, because it checks the destination, but the
# `Lance` object store must not.
grep -c 'ClickHouse-Lance' "${TARGET_LOG}" || true
