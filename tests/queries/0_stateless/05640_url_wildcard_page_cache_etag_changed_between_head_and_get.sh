#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs a local HTTP server serving index pages

# The page cache keeps the bytes of a URL wildcard read under the strong `ETag` from the `HEAD` request, so the `GET`
# that fills it must return the same generation of the object. Here the object changes between the `HEAD` and the
# `GET` and is restored afterwards: the read must fail instead of caching the other bytes under the old `ETag`, and
# the next reads must see the original contents (the last one from the page cache).
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

# `/d/data.csv` is the first version with `ETag` \"a\". After a request to `/arm`, the next `GET` of it returns
# the second version (of the same size) with `ETag` \"b\", as if the object was overwritten after the `HEAD`
# and restored after the `GET`.
python3 -c "
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler

VERSIONS = [(b'1,aaaa\n' * 1000, '\"a\"'), (b'2,bbbb\n' * 1000, '\"b\"')]
state = {'armed': False}

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def respond(self, with_body):
        path = self.path.split('?')[0]
        etag = None
        if path == '/arm':
            if with_body:
                state['armed'] = True
            data, content_type = b'ok', 'text/plain'
        elif path == '/d/':
            data, content_type = b'<a href=\"data.csv\">data.csv</a>\n', 'text/html'
        elif path == '/d/data.csv':
            version = 0
            if with_body and state['armed']:
                state['armed'] = False
                version = 1
            data, etag = VERSIONS[version]
            content_type = 'text/csv'
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
    curl -s "http://127.0.0.1:$HTTP_PORT/d/" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

base="http://127.0.0.1:${HTTP_PORT}"
settings="use_page_cache_for_object_storage = 1, allow_url_wildcard_from_index_pages = 1, http_max_tries = 1"

# `--ignore-error` continues after the failing read in the same process, which holds the page cache.
${CLICKHOUSE_LOCAL} --path "${work_dir}/db" --config-file "${work_dir}/config.yaml" --log_queries 1 --ignore-error --query "
    SELECT * FROM url('${base}/arm', RawBLOB);
    SELECT 'changed', sum(c1), any(c2) FROM url('${base}/d/*.csv', CSV, 'c1 UInt64, c2 String')
        SETTINGS ${settings}, log_comment = 'read_1';
    SELECT 'restored', sum(c1), any(c2) FROM url('${base}/d/*.csv', CSV, 'c1 UInt64, c2 String')
        SETTINGS ${settings}, log_comment = 'read_2';
    SELECT 'restored', sum(c1), any(c2) FROM url('${base}/d/*.csv', CSV, 'c1 UInt64, c2 String')
        SETTINGS ${settings}, log_comment = 'read_3';
    SYSTEM FLUSH LOGS query_log;
    SELECT log_comment, type, errorCodeToName(exception_code), ProfileEvents['PageCacheHits'] > 0
        FROM system.query_log
        WHERE current_database = currentDatabase() AND type != 'QueryStart' AND log_comment IN ('read_1', 'read_2', 'read_3')
        ORDER BY log_comment;" 2>"${work_dir}/stderr"

grep -q -F "FILE_CHANGED_DURING_READ" "${work_dir}/stderr" && echo "the changed read failed with FILE_CHANGED_DURING_READ"

rm -rf "${work_dir}"
