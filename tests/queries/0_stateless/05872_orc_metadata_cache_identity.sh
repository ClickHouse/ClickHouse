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
${CLICKHOUSE_CLIENT} --query "SELECT number + 100 AS id FROM numbers(10) FORMAT ORC" > "${work_dir}/changed.orc"
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
            data, etag = b'ok', None
        elif self.path == '/arm' and self.server.origin == 0:
            self.server.armed = True
            data, etag = b'ok', None
        elif self.path == '/deny' and self.server.origin == 0:
            self.server.denied = True
            data, etag = b'ok', None
        elif getattr(self.server, 'failed', False):
            self.send_response(503)
            self.send_header('Content-Length', '0')
            self.end_headers()
            return
        elif self.path == '/data/':
            data = b'<a href="f.orc">f.orc</a>'
            etag = None
        elif self.path == '/data/f.orc' and self.server.origin == 0 and getattr(self.server, 'denied', False) and include_body:
            self.send_error(403)
            return
        elif self.path == '/data/f.orc':
            name = 'small.orc' if self.server.origin == 0 else 'large.orc'
            data = (work_dir / name).read_bytes()
            etag = '"same-etag"'
            if self.server.origin == 0 and getattr(self.server, 'armed', False) and include_body:
                data, etag = (work_dir / 'changed.orc').read_bytes(), '"changed"'
                self.server.armed = False
        elif self.path == '/failover/':
            data = b'<a href="f.orc">f.orc</a>'
            etag = None
        elif self.path == '/failover/f.orc':
            name = 'small.orc' if self.server.origin == 0 else 'large.orc'
            data = (work_dir / name).read_bytes()
            etag = f'"origin-{self.server.origin}"'
        elif self.path == '/redirect/':
            data = b'<a href="f.orc">f.orc</a>'
            etag = None
        elif self.path == '/redirect/f.orc':
            port = self.server.server_port
            self.send_response(302)
            self.send_header('Location', f'http://127.0.0.1:{port}/redirect-target/f.orc')
            self.send_header('Content-Length', '0')
            self.end_headers()
            return
        elif self.path == '/redirect-target/f.orc':
            data = (work_dir / 'small.orc').read_bytes()
            etag = '"redirect-etag"'
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

settings="use_orc_metadata_cache = 1, use_cache_for_count_from_files = 0, use_page_cache_for_object_storage = 0, allow_url_wildcard_from_index_pages = 1, http_max_tries = 1"
${CLICKHOUSE_CLIENT} --query "SYSTEM DROP ORC METADATA CACHE"
for port in "$first_port" "$second_port"; do
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM url('http://127.0.0.1:${port}/data/*.orc', 'ORC', 'id UInt64') SETTINGS ${settings}"
done

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM url('http://127.0.0.1:${first_port}/weak/*.orc', 'ORC', 'id UInt64') SETTINGS ${settings}"
cp "${work_dir}/large.orc" "${work_dir}/weak.orc"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM url('http://127.0.0.1:${first_port}/weak/*.orc', 'ORC', 'id UInt64') SETTINGS ${settings}"

${CLICKHOUSE_CLIENT} --query "SELECT count(), any(_headers['X-Method']) FROM url('http://127.0.0.1:${first_port}/headers/*.orc', 'ORC', 'id UInt64') SETTINGS ${settings}, remote_filesystem_read_prefetch = 0"
${CLICKHOUSE_CLIENT} --query "SELECT count(), any(_headers['X-Method']) FROM url('http://127.0.0.1:${first_port}/headers/*.orc', 'ORC', 'id UInt64') SETTINGS ${settings}, remote_filesystem_read_prefetch = 0"

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM url('http://127.0.0.1:${first_port}/redirect/*.orc', 'ORC', 'id UInt64') SETTINGS ${settings}, max_http_get_redirects = 1"

failover_url="http://127.0.0.1:{${first_port}|${second_port}}/failover/*.orc"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM url('${failover_url}', 'ORC', 'id UInt64') SETTINGS ${settings}"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM url('http://127.0.0.1:${first_port}/fail', 'RawBLOB')"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM url('${failover_url}', 'ORC', 'id UInt64') SETTINGS ${settings}"

# An object changed between metadata and data requests must not populate the old ETag key.
${CLICKHOUSE_CLIENT} --query "SELECT * FROM url('http://127.0.0.1:${first_port}/arm', 'RawBLOB')"
if ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM url('http://127.0.0.1:${first_port}/data/*.orc', 'ORC', 'id UInt64') SETTINGS ${settings}" > /dev/null 2> "${work_dir}/changed.err"; then
    echo "unexpectedly allowed"
    exit 1
fi
grep -q 'FILE_CHANGED_DURING_READ' "${work_dir}/changed.err"
echo "rejected"

# A warm tail must not bypass the current reader's object-read authorization.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM url('http://127.0.0.1:${first_port}/data/*.orc', 'ORC', 'id UInt64') SETTINGS ${settings}"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM url('http://127.0.0.1:${first_port}/deny', 'RawBLOB')"
if ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM url('http://127.0.0.1:${first_port}/data/*.orc', 'ORC', 'id UInt64') SETTINGS ${settings}" > /dev/null 2> "${work_dir}/denied.err"; then
    echo "unexpectedly allowed"
    exit 1
fi
grep -qE '403|ACCESS_DENIED' "${work_dir}/denied.err"
echo "denied"
