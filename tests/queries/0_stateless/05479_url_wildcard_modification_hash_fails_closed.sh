#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: starts a local HTTP server with python3.

# A listable HTTP `URL` table (wildcard from index pages) is read through `WebObjectStorage`, which
# follows redirects without keeping the final resource. A stable index entry `part.tsv` can redirect
# first to `/v1` and then to `/v2`, both with the same strong `ETag`, size and `Last-Modified`, so the
# relative path and the validators do not move although the data changed. `modification_hash` must
# therefore be NULL for such a table, while the plain `URL` engine folds the final resource in
# (see `05457_url_modification_hash_encoding_and_redirect`).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

HTTP_PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

python3 -c "
import sys
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

state = {'version': 1}

class Handler(BaseHTTPRequestHandler):
    def respond(self, with_body):
        path = self.path.split('?')[0]
        if path == '/flip':
            state['version'] += 1
            code, headers, body = 200, {}, b'ok'
        elif path == '/data/':
            code, headers, body = 200, {'Content-Type': 'text/html'}, b'<a href=\"part.tsv\">part.tsv</a>\n'
        elif path == '/data/part.tsv':
            code, headers, body = 302, {'Location': '/v%d' % state['version']}, b''
        elif path in ('/v1', '/v2'):
            # Different resources sharing a strong validator.
            code, headers, body = 200, {'ETag': '\"1\"', 'Last-Modified': 'Thu, 01 Jan 2026 00:00:00 GMT'}, (path[2:] + '\n').encode()
        else:
            code, headers, body = 404, {}, b''
        self.send_response(code)
        for k, v in headers.items():
            self.send_header(k, v)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        if with_body:
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass
    def do_GET(self):
        self.respond(True)
    def do_HEAD(self):
        self.respond(False)
    def log_message(self, *args):
        pass

ThreadingHTTPServer(('127.0.0.1', int(sys.argv[1])), Handler).serve_forever()
" "${HTTP_PORT}" &
HTTP_PID=$!
trap "kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null" EXIT

for _ in $(seq 1 50); do
    curl -s "http://127.0.0.1:$HTTP_PORT/data/" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

table="${CLICKHOUSE_TEST_UNIQUE_NAME}_t"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${table}"
${CLICKHOUSE_CLIENT} --allow_url_wildcard_from_index_pages 1 -q "CREATE TABLE ${table} (x UInt64) ENGINE = URL('http://127.0.0.1:${HTTP_PORT}/data/*.tsv', 'TSV')"

${CLICKHOUSE_CLIENT} -q "SELECT 'v1 read', x FROM ${table}"
${CLICKHOUSE_CLIENT} -q "SELECT 'v1 hash is null', modification_hash IS NULL FROM system.tables WHERE database = currentDatabase() AND name = '${table}'"

curl -s "http://127.0.0.1:$HTTP_PORT/flip" -o /dev/null

${CLICKHOUSE_CLIENT} -q "SELECT 'v2 read', x FROM ${table}"
${CLICKHOUSE_CLIENT} -q "SELECT 'v2 hash is null', modification_hash IS NULL FROM system.tables WHERE database = currentDatabase() AND name = '${table}'"

${CLICKHOUSE_CLIENT} -q "DROP TABLE ${table}"
