#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Parquet and a local HTTP server serving index pages

# The query condition cache key of an object read without a table UUID must not carry secrets: a web
# index page can link to a child with a signed query (`data.parquet?token=...`), and the key (the
# `part_name`) is visible in the logs and in `system.query_condition_cache` of debug builds. The key
# keeps only the path without the query, followed by a hash of the complete location.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_FOLDER="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${WORKING_FOLDER}"
mkdir -p "${WORKING_FOLDER}"

# All values are even, so the odd value 3 never matches and every row group is recorded as without a match.
${CLICKHOUSE_LOCAL} --query "
    SELECT number * 2 AS b FROM numbers(1000000) INTO OUTFILE '${WORKING_FOLDER}/data.parquet' FORMAT Parquet
    SETTINGS output_format_parquet_row_group_size = 100000"

HTTP_PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

python3 -c "
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler

DATA = open(sys.argv[1], 'rb').read()
INDEX = b'<a href=\"data.parquet?token=very_secret_token\">data.parquet</a>\n'

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def respond(self, with_body):
        path = self.path.split('?')[0]
        if path == '/data/':
            data, content_type = INDEX, 'text/html'
        elif path == '/data/data.parquet':
            data, content_type = DATA, 'application/octet-stream'
        else:
            self.send_response(404)
            self.end_headers()
            return
        rng = self.headers.get('Range')
        status = 200
        if rng and path != '/data/':
            begin, end = rng.split('=')[1].split('-')
            begin = int(begin)
            end = int(end) if end else len(data) - 1
            content_range = 'bytes %d-%d/%d' % (begin, end, len(data))
            data = data[begin:end + 1]
            status = 206
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Accept-Ranges', 'bytes')
        if status == 206:
            self.send_header('Content-Range', content_range)
        self.send_header('ETag', '\"05401\"')
        self.end_headers()
        if with_body:
            try:
                self.wfile.write(data)
            except (BrokenPipeError, ConnectionResetError):
                # The reader may close the connection before the end of a range it does not need.
                pass

    def do_HEAD(self):
        self.respond(False)

    def do_GET(self):
        self.respond(True)

HTTPServer(('127.0.0.1', int(sys.argv[2])), Handler).serve_forever()
" "${WORKING_FOLDER}/data.parquet" "${HTTP_PORT}" &
HTTP_PID=$!
trap "kill $HTTP_PID 2>/dev/null; wait $HTTP_PID 2>/dev/null" EXIT

for _ in $(seq 1 50); do
    curl -s "http://127.0.0.1:$HTTP_PORT/data/" -o /dev/null 2>/dev/null && break
    sleep 0.1
done

QUERY="SELECT count() FROM url('http://127.0.0.1:${HTTP_PORT}/data/*.parquet', Parquet) WHERE b = 3
    SETTINGS use_query_condition_cache = 1, allow_url_wildcard_from_index_pages = 1, http_max_tries = 1"
${CLICKHOUSE_LOCAL} --send_logs_level=test --query "${QUERY}" 2> "${WORKING_FOLDER}/stderr" \
    || cat "${WORKING_FOLDER}/stderr"

# The cache logs every access with its key (`-a`: the key contains a NUL). The lookup and the entry use
# the path without the token, and the token is nowhere in the keys.
grep -a 'QueryConditionCache' "${WORKING_FOLDER}/stderr" | grep -a -c 'Could not find entry for table_uuid: .*part: .*data/data.parquet#storage='
grep -a 'QueryConditionCache' "${WORKING_FOLDER}/stderr" | grep -a -c 'Inserted entry for table_id: .*part_name: .*data/data.parquet#storage='
grep -a 'QueryConditionCache' "${WORKING_FOLDER}/stderr" | grep -a -c 'very_secret_token'

rm -rf "${WORKING_FOLDER}"
