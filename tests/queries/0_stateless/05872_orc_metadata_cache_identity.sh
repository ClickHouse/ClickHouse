#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings, no-parallel
# no-fasttest: requires an `ORC`-enabled build and a local HTTP server.
# no-random-settings: the cache and count settings are part of the regression.
# no-parallel: the test clears the process-wide ORC metadata cache.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

mkdir -p tmp
work_dir=$(mktemp -d "tmp/05872_${CLICKHOUSE_TEST_UNIQUE_NAME}.XXXXXX")
trap 'kill "$http_pid" 2>/dev/null || true; wait "$http_pid" 2>/dev/null || true; rm -rf "$work_dir"' EXIT

${CLICKHOUSE_CLIENT} --query "SELECT number AS id FROM numbers(10) FORMAT ORC" > "${work_dir}/small.orc"
${CLICKHOUSE_CLIENT} --query "SELECT number AS id FROM numbers(20) FORMAT ORC" > "${work_dir}/large.orc"
cp "${work_dir}/small.orc" "${work_dir}/weak.orc"

python3 -u - "${work_dir}" <<'PY' &
import pathlib
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

work_dir = pathlib.Path(sys.argv[1])

class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def respond(self, include_body):
        if self.path == '/fail':
            self.server.failed = True
            data = b'ok'
            etag = None
        elif getattr(self.server, 'failed', False):
            self.send_response(503)
            self.send_header('Content-Length', '0')
            self.end_headers()
            return
        elif self.path == '/data/':
            data = b'<a href="f.orc">f.orc</a>'
            etag = None
        elif self.path == '/data/f.orc':
            name = 'small.orc' if self.server.origin == 0 else 'large.orc'
            data = (work_dir / name).read_bytes()
            etag = '"same-etag"'
        elif self.path == '/weak/':
            data = b'<a href="f.orc">f.orc</a>'
            etag = None
        elif self.path == '/weak/f.orc':
            data = (work_dir / 'weak.orc').read_bytes()
            etag = 'W/"same-etag"'
        elif self.path == '/headers/':
            data = b'<a href="f.orc">f.orc</a>'
            etag = None
        elif self.path == '/headers/f.orc':
            data = (work_dir / 'small.orc').read_bytes()
            etag = '"headers-etag"'
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
        self.send_header('X-Method', 'GET' if include_body else 'HEAD')
        if content_range:
            self.send_header('Content-Range', content_range)
        if etag:
            self.send_header('ETag', etag)
        self.end_headers()
        if include_body:
            self.wfile.write(data)

    def do_HEAD(self):
        self.respond(False)

    def do_GET(self):
        self.respond(True)

    def log_message(self, *args):
        pass

servers = []
for origin in range(2):
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    server.origin = origin
    servers.append(server)
    threading.Thread(target=server.serve_forever, daemon=True).start()
(work_dir / 'ports').write_text(' '.join(str(server.server_port) for server in servers) + '\n')
threading.Event().wait()
PY
http_pid=$!

for _ in $(seq 1 100); do
    if [[ -s "${work_dir}/ports" ]]; then break; fi
    sleep 0.1
done
[[ -s "${work_dir}/ports" ]]
read -r first_port second_port < "${work_dir}/ports"

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP ORC METADATA CACHE"
for port in "$first_port" "$second_port"; do
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM url('http://127.0.0.1:${port}/data/*.orc', 'ORC', 'id UInt64') SETTINGS use_orc_metadata_cache = 1, use_cache_for_count_from_files = 0, allow_url_wildcard_from_index_pages = 1"
done

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM url('http://127.0.0.1:${first_port}/weak/*.orc', 'ORC', 'id UInt64') SETTINGS use_orc_metadata_cache = 1, use_cache_for_count_from_files = 0, allow_url_wildcard_from_index_pages = 1"
cp "${work_dir}/large.orc" "${work_dir}/weak.orc"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM url('http://127.0.0.1:${first_port}/weak/*.orc', 'ORC', 'id UInt64') SETTINGS use_orc_metadata_cache = 1, use_cache_for_count_from_files = 0, allow_url_wildcard_from_index_pages = 1"

${CLICKHOUSE_CLIENT} --query "SELECT count(), any(_headers['X-Method']) FROM url('http://127.0.0.1:${first_port}/headers/*.orc', 'ORC', 'id UInt64') SETTINGS use_orc_metadata_cache = 1, use_cache_for_count_from_files = 0, allow_url_wildcard_from_index_pages = 1, remote_filesystem_read_prefetch = 0"
${CLICKHOUSE_CLIENT} --query "SELECT count(), any(_headers['X-Method']) FROM url('http://127.0.0.1:${first_port}/headers/*.orc', 'ORC', 'id UInt64') SETTINGS use_orc_metadata_cache = 1, use_cache_for_count_from_files = 0, allow_url_wildcard_from_index_pages = 1, remote_filesystem_read_prefetch = 0"

failover_url="http://127.0.0.1:{${first_port}|${second_port}}/data/*.orc"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM url('${failover_url}', 'ORC', 'id UInt64') SETTINGS use_orc_metadata_cache = 1, use_cache_for_count_from_files = 0, use_page_cache_for_object_storage = 1, allow_url_wildcard_from_index_pages = 1"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM url('http://127.0.0.1:${first_port}/fail', 'RawBLOB')"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM url('${failover_url}', 'ORC', 'id UInt64') SETTINGS use_orc_metadata_cache = 1, use_cache_for_count_from_files = 0, use_page_cache_for_object_storage = 1, allow_url_wildcard_from_index_pages = 1"
