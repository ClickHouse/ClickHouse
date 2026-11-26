#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings, no-parallel
# no-fasttest: requires an `ORC`-enabled build and a local HTTP server.
# no-random-settings: the cache and count settings are part of the regression.
# no-parallel: the test clears the process-wide ORC metadata cache.

# A metadata-cache hit must not disclose a footer when the current reader's
# data `GET` is denied. The metadata `HEAD` remains allowed, just as a storage
# listing can succeed while reading the object is forbidden.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

mkdir -p tmp
work_dir=$(mktemp -d "tmp/05876_${CLICKHOUSE_TEST_UNIQUE_NAME}.XXXXXX")
trap 'kill "$http_pid" 2>/dev/null || true; wait "$http_pid" 2>/dev/null || true; rm -rf "$work_dir"' EXIT

${CLICKHOUSE_CLIENT} --query "SELECT number AS id FROM numbers(10) FORMAT ORC" > "${work_dir}/small.orc"

python3 -u - "${work_dir}" <<'PY' &
import pathlib
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

work_dir = pathlib.Path(sys.argv[1])
deny_get = False

class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def respond(self, include_body):
        global deny_get
        if self.path == '/deny':
            if include_body:
                deny_get = True
            data = b'ok'
        elif include_body and deny_get and self.path == '/data/f.orc':
            self.send_error(403)
            return
        elif self.path == '/data/':
            if include_body:
                self.server.listing_gets += 1
            data = b'<a href="f.orc">f.orc</a>'
        elif self.path == '/data/f.orc':
            if include_body and deny_get:
                self.send_error(403)
                return
            self.server.data_gets += 1
            data = (work_dir / 'small.orc').read_bytes()
        else:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Accept-Ranges', 'bytes')
        self.send_header('ETag', '"same-etag"')
        self.end_headers()
        if include_body:
            self.wfile.write(data)

    def do_HEAD(self):
        self.respond(False)

    def do_GET(self):
        self.respond(True)

    def do_POST(self):
        length = int(self.headers.get('Content-Length', 0))
        self.rfile.read(length)
        query = parse_qs(urlsplit(self.path).query)
        if query.get('query') == ['SELECT version()']:
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('X-ClickHouse-Summary', '{"read_rows":"0","read_bytes":"0"}')
            self.send_header('Content-Length', '0')
            self.end_headers()
            (work_dir / 'requests.json').write_text(
                f'{{"listing_gets": {server.listing_gets}, "data_gets": {server.data_gets}}}\n')
        else:
            self.send_error(400)

    def log_message(self, *args):
        pass

server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
server.listing_gets = 0
server.data_gets = 0
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
settings="use_orc_metadata_cache = 1, use_cache_for_count_from_files = 0, use_page_cache_for_object_storage = 0, remote_filesystem_read_prefetch = 0, allow_url_wildcard_from_index_pages = 1, http_max_tries = 1"

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM url('http://127.0.0.1:${port}/data/*.orc', 'ORC', 'id UInt64') SETTINGS ${settings}"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM url('http://127.0.0.1:${port}/deny', 'RawBLOB')"
if ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM url('http://127.0.0.1:${port}/data/*.orc', 'ORC', 'id UInt64') SETTINGS ${settings}" > "${work_dir}/denied.out" 2> "${work_dir}/denied.err"; then
    echo "unexpectedly allowed"
    exit 1
fi
grep -qE '403|ACCESS_DENIED' "${work_dir}/denied.err"
${CLICKHOUSE_CURL} -X POST --data-binary 'SELECT version()' "http://127.0.0.1:${port}/?query=SELECT%20version()" > /dev/null
python3 - "${work_dir}/requests.json" <<'PY'
import json
import sys
requests = json.load(open(sys.argv[1]))
assert requests['listing_gets'] == 2, requests
assert requests['data_gets'] >= 1, requests
PY
echo "denied"
