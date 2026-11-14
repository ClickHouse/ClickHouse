#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs local HTTP servers serving index pages

# Two URL shards list an object with the same path, size and strong `ETag`, but different contents. The connection of
# a `Web` object storage to the distributed cache has only the first base URL, so a read through the distributed cache
# would read both objects from the first shard. Such reads go directly to the origin instead, even with
# `table_engine_read_through_distributed_cache = 1`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

read -r PORT1 PORT2 < <(python3 -c "
import socket
sockets = [socket.socket() for _ in range(2)]
for s in sockets:
    s.bind(('127.0.0.1', 0))
print(*[s.getsockname()[1] for s in sockets])
")

# The shard on the first port serves `1`, the one on the second port serves `2`.
python3 -c "
import sys, threading
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

def make_handler(value):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def respond(self, with_body):
            path = self.path.split('?')[0]
            etag = None
            if path == '/d/':
                data, content_type = b'<a href=\"f.tsv\">f.tsv</a>\n', 'text/html'
            elif path == '/d/f.tsv':
                data, content_type, etag = value, 'text/tab-separated-values', '\"same\"'
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
    return Handler

servers = [ThreadingHTTPServer(('127.0.0.1', int(port)), make_handler(value))
           for port, value in ((sys.argv[1], b'1\n'), (sys.argv[2], b'2\n'))]
threads = [threading.Thread(target=server.serve_forever) for server in servers]
for thread in threads:
    thread.start()
for thread in threads:
    thread.join()
" "${PORT1}" "${PORT2}" &
HTTP_PID=$!
trap "kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null" EXIT

for port in "${PORT1}" "${PORT2}"; do
    for _ in $(seq 1 50); do
        curl -s "http://127.0.0.1:${port}/d/" -o /dev/null 2>/dev/null && break
        sleep 0.1
    done
done

for i in 1 2; do
    ${CLICKHOUSE_CLIENT} --query_id "${CLICKHOUSE_TEST_UNIQUE_NAME}_${i}" --query "
        SELECT c FROM url('http://127.0.0.1:{${PORT1},${PORT2}}/d/*.tsv', TSV, 'c UInt64') ORDER BY c
        SETTINGS allow_url_wildcard_from_index_pages = 1, table_engine_read_through_distributed_cache = 1,
            use_page_cache_for_object_storage = 0, http_max_tries = 1"
done

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), sum(ProfileEvents['DistrCacheReceivedDataPacketsBytes'])
    FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
        AND startsWith(query_id, '${CLICKHOUSE_TEST_UNIQUE_NAME}_')"
