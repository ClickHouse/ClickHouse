#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs a local HTTP server serving index pages

# A weak HTTP `ETag` (`W/"..."`) allows different bytes of the same representation (RFC 9110, section 8.8.1),
# so it must not key the page cache: a URL wildcard read twice across a same-size change of the object that
# keeps its weak `ETag` must see the new contents. With a strong `ETag`, the second read of an unchanged object
# is served from the page cache, which shows that the page cache is in use.
#
# The CI server configures no page cache, so everything runs in a private clickhouse-local that has one.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

work_dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_page_cache"
rm -rf "${work_dir}"
mkdir -p "${work_dir}"

> "${work_dir}/config.yaml" echo "
page_cache_max_size: 134217728
query_log:
    database: system
    table: query_log
"

HTTP_PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

# `/weak/data.csv` and `/strong/data.csv` have the same size in both versions. A request to `/switch`
# changes both to the second version, which keeps the same `ETag`.
python3 -c "
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler

VERSIONS = [b'1,aaaa\n' * 1000, b'2,bbbb\n' * 1000]
state = {'version': 0}

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def respond(self, with_body):
        path = self.path.split('?')[0]
        etag = None
        if path == '/switch':
            if with_body:
                state['version'] = 1
            data, content_type = b'ok', 'text/plain'
        elif path in ('/weak/', '/strong/'):
            data, content_type = b'<a href=\"data.csv\">data.csv</a>\n', 'text/html'
        elif path in ('/weak/data.csv', '/strong/data.csv'):
            data, content_type = VERSIONS[state['version']], 'text/csv'
            etag = 'W/\"v1\"' if path.startswith('/weak/') else '\"v1\"'
        else:
            self.send_response(404)
            self.end_headers()
            return
        rng = self.headers.get('Range')
        status = 200
        if rng and etag is not None:
            begin, end = rng.split('=')[1].split('-')
            begin = int(begin)
            end = min(int(end), len(data) - 1) if end else len(data) - 1
            content_range = 'bytes %d-%d/%d' % (begin, end, len(data))
            data = data[begin:end + 1]
            status = 206
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Accept-Ranges', 'bytes')
        if status == 206:
            self.send_header('Content-Range', content_range)
        if etag is not None:
            self.send_header('ETag', etag)
        self.end_headers()
        if with_body:
            try:
                self.wfile.write(data)
            except (BrokenPipeError, ConnectionResetError):
                pass

    def do_HEAD(self):
        self.respond(False)

    def do_GET(self):
        self.respond(True)

HTTPServer(('127.0.0.1', int(sys.argv[1])), Handler).serve_forever()
" "${HTTP_PORT}" &
HTTP_PID=$!
trap "kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null" EXIT

for _ in $(seq 1 50); do
    curl -s "http://127.0.0.1:$HTTP_PORT/weak/" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

base="http://127.0.0.1:${HTTP_PORT}"
settings="use_page_cache_for_object_storage = 1, allow_url_wildcard_from_index_pages = 1, http_max_tries = 1"

${CLICKHOUSE_LOCAL} --path "${work_dir}/db" --config-file "${work_dir}/config.yaml" --log_queries 1 --query "
    SELECT 'strong', sum(c1), any(c2) FROM url('${base}/strong/*.csv', CSV, 'c1 UInt64, c2 String')
        SETTINGS ${settings}, log_comment = 'strong_1';
    SELECT 'strong', sum(c1), any(c2) FROM url('${base}/strong/*.csv', CSV, 'c1 UInt64, c2 String')
        SETTINGS ${settings}, log_comment = 'strong_2';
    SELECT 'weak', sum(c1), any(c2) FROM url('${base}/weak/*.csv', CSV, 'c1 UInt64, c2 String')
        SETTINGS ${settings}, log_comment = 'weak_1';
    SELECT * FROM url('${base}/switch', RawBLOB);
    SELECT 'weak', sum(c1), any(c2) FROM url('${base}/weak/*.csv', CSV, 'c1 UInt64, c2 String')
        SETTINGS ${settings}, log_comment = 'weak_2';
    SYSTEM FLUSH LOGS query_log;
    SELECT log_comment, ProfileEvents['PageCacheHits'] > 0 FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('strong_1', 'strong_2', 'weak_1', 'weak_2')
        ORDER BY log_comment;"

rm -rf "${work_dir}"
