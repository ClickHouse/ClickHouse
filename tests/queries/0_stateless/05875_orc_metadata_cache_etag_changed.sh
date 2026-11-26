#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings, no-parallel
# no-fasttest: requires an `ORC`-enabled build and a local HTTP server.
# no-random-settings: the cache and count settings are part of the regression.
# no-parallel: the test clears the process-wide ORC metadata cache.

# The tail `GET` must be pinned to the strong `ETag` seen at metadata time,
# even when the page cache is disabled. A replacement between `HEAD` and `GET`
# must fail instead of caching another file under the old key.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

mkdir -p tmp
work_dir=$(mktemp -d "tmp/05875_${CLICKHOUSE_TEST_UNIQUE_NAME}.XXXXXX")
trap 'kill "$http_pid" 2>/dev/null || true; wait "$http_pid" 2>/dev/null || true; rm -rf "$work_dir"' EXIT

${CLICKHOUSE_CLIENT} --query "SELECT number AS id FROM numbers(10) FORMAT ORC" > "${work_dir}/small.orc"
${CLICKHOUSE_CLIENT} --query "SELECT number + 100 AS id FROM numbers(10) FORMAT ORC" > "${work_dir}/changed.orc"
cp "${work_dir}/small.orc" "${work_dir}/data.orc"

python3 -u - "${work_dir}" <<'PY' &
import pathlib
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

work_dir = pathlib.Path(sys.argv[1])
armed = False

class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def respond(self, include_body):
        global armed
        etag = None
        if self.path == '/arm':
            if include_body:
                armed = True
            data = b'ok'
        elif self.path == '/data/':
            data = b'<a href="f.orc">f.orc</a>'
        elif self.path == '/data/f.orc':
            name = 'small.orc'
            etag = '"a"'
            if include_body and armed:
                name = 'changed.orc'
                etag = '"b"'
                armed = False
            data = (work_dir / name).read_bytes()
        else:
            self.send_error(404)
            return
        range_header = self.headers.get('Range')
        content_range = None
        status = 200
        if range_header and etag:
            begin, end = range_header.split('=')[1].split('-')
            begin = int(begin)
            end = min(int(end), len(data) - 1) if end else len(data) - 1
            content_range = f'bytes {begin}-{end}/{len(data)}'
            data = data[begin:end + 1]
            status = 206
        self.send_response(status)
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Accept-Ranges', 'bytes')
        if content_range:
            self.send_header('Content-Range', content_range)
        if etag:
            self.send_header('ETag', etag)
        self.end_headers()
        if include_body:
            try:
                self.wfile.write(data)
            except (BrokenPipeError, ConnectionResetError):
                pass

    def do_HEAD(self):
        self.respond(False)

    def do_GET(self):
        self.respond(True)

    def log_message(self, *args):
        pass

    def handle_one_request(self):
        try:
            super().handle_one_request()
        except ConnectionResetError:
            pass

server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
(work_dir / 'port').write_text(str(server.server_port) + '\n')
server.serve_forever()
PY
http_pid=$!

for _ in $(seq 1 100); do
    if [[ -s "${work_dir}/port" ]]; then break; fi
    sleep 0.1
done
[[ -s "${work_dir}/port" ]]
port=$(<"${work_dir}/port")
settings="use_orc_metadata_cache = 1, use_cache_for_count_from_files = 0, use_page_cache_for_object_storage = 0, allow_url_wildcard_from_index_pages = 1, http_max_tries = 1"

${CLICKHOUSE_CLIENT} --query "SELECT * FROM url('http://127.0.0.1:${port}/arm', 'RawBLOB')"
if ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM url('http://127.0.0.1:${port}/data/*.orc', 'ORC', 'id UInt64') SETTINGS ${settings}" > "${work_dir}/changed.out" 2> "${work_dir}/changed.err"; then
    echo "unexpectedly allowed"
    exit 1
fi
grep -q 'FILE_CHANGED_DURING_READ' "${work_dir}/changed.err" || { printf '%s\n' '--- changed read stderr ---'; /bin/cat "${work_dir}/changed.err"; exit 1; }
echo "rejected"

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM url('http://127.0.0.1:${port}/data/*.orc', 'ORC', 'id UInt64') SETTINGS ${settings}"
