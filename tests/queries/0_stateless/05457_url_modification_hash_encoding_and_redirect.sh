#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: starts a local HTTP server with python3.

# `system.tables.modification_hash` of a `URL` table must follow the resource that the read uses:
# - the probe requests the path with the same URL encoding as the reader, so `/test%2Fa.tsv` is not
#   probed as `/test/a.tsv`, which can be a different resource;
# - when the URL redirects, the final resource is part of the hash, so switching the redirect to
#   another resource with the same strong `ETag` changes the hash.

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
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

state = {'version': 1}

class Handler(BaseHTTPRequestHandler):
    def respond(self, with_body):
        path = self.path
        if path == '/flip':
            state['version'] += 1
            code, headers, body = 200, {}, b'ok'
        elif path == '/current':
            code, headers, body = 302, {'Location': '/v%d' % state['version']}, b''
        elif path in ('/v1', '/v2'):
            # Different resources sharing a strong validator.
            code, headers, body = 200, {'ETag': '\"1\"'}, (path[2:] + '\n').encode()
        elif path == '/test%2Fa.tsv':
            code, headers, body = 200, {'ETag': '\"literal-%d\"' % state['version']}, b'%d\n' % state['version']
        elif path == '/test/a.tsv':
            code, headers, body = 200, {'ETag': '\"decoded\"'}, b'0\n'
        else:
            code, headers, body = 404, {}, b''
        self.send_response(code)
        for k, v in headers.items():
            self.send_header(k, v)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        if with_body:
            self.wfile.write(body)
    def do_GET(self):
        self.respond(True)
    def do_HEAD(self):
        self.respond(False)
    def log_message(self, *args):
        pass

ThreadingHTTPServer(('127.0.0.1', $HTTP_PORT), Handler).serve_forever()
" &
HTTP_PID=$!
trap "kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null" EXIT

for _ in $(seq 1 100); do
    curl -s "http://127.0.0.1:$HTTP_PORT/v1" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE IF EXISTS t_url_encoded;
    DROP TABLE IF EXISTS t_url_redirect;
    CREATE TABLE t_url_encoded (x UInt64) ENGINE = URL('http://127.0.0.1:$HTTP_PORT/test%2Fa.tsv', 'TSV');
    CREATE TABLE t_url_redirect (x UInt64) ENGINE = URL('http://127.0.0.1:$HTTP_PORT/current', 'TSV');
"

function hashes()
{
    ${CLICKHOUSE_CLIENT} -q "
        SELECT name, modification_hash FROM system.tables
        WHERE database = currentDatabase() AND name IN ('t_url_encoded', 't_url_redirect')
        ORDER BY name
        SETTINGS max_http_get_redirects = 1"
}

hashes > "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_before"
${CLICKHOUSE_CLIENT} -q "SELECT 'read before', (SELECT x FROM t_url_encoded), (SELECT x FROM t_url_redirect SETTINGS max_http_get_redirects = 1)"

curl -s "http://127.0.0.1:$HTTP_PORT/flip" -o /dev/null

hashes > "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_after"
${CLICKHOUSE_CLIENT} -q "SELECT 'read after', (SELECT x FROM t_url_encoded), (SELECT x FROM t_url_redirect SETTINGS max_http_get_redirects = 1)"

paste "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_before" "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_after" \
    | awk -F'\t' '{ print $1, ($2 == "\\N" ? "null" : "not null"), ($2 != $4 ? "changed" : "unchanged") }'

${CLICKHOUSE_CLIENT} -q "
    DROP TABLE t_url_encoded;
    DROP TABLE t_url_redirect;
"
rm -f "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_before" "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_after"
