#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings
# no-fasttest: requires a Parquet-enabled build and a local HTTP server.
# no-random-settings: the cache, prefetch and physical-column settings are the regression.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

mkdir -p tmp
work_dir=$(mktemp -d "tmp/05878_${CLICKHOUSE_TEST_UNIQUE_NAME}.XXXXXX")
trap 'kill "$http_pid" 2>/dev/null || true; wait "$http_pid" 2>/dev/null || true; rm -rf "$work_dir"' EXIT

${CLICKHOUSE_CLIENT} --query "SELECT number AS id FROM numbers(10) FORMAT Parquet" > "${work_dir}/data.parquet"

python3 -u - "${work_dir}" <<'PY' &
import pathlib
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

work_dir = pathlib.Path(sys.argv[1])


class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def respond(self, include_body):
        if self.path == '/data/':
            data = b'<a href="data.parquet">data.parquet</a>'
        elif self.path == '/data/data.parquet':
            data = (work_dir / 'data.parquet').read_bytes()
        else:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Accept-Ranges', 'bytes')
        self.send_header('ETag', '"stable"')
        self.send_header('X-Method', 'GET' if include_body else 'HEAD')
        self.end_headers()
        if include_body:
            self.wfile.write(data)

    do_GET = lambda self: self.respond(True)
    do_HEAD = lambda self: self.respond(False)

    def log_message(self, *args):
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
settings="remote_filesystem_read_prefetch = 0, use_page_cache_for_object_storage = 0, use_cache_for_count_from_files = 0, use_parquet_metadata_cache = 1, input_format_parquet_allow_missing_columns = 1, allow_url_wildcard_from_index_pages = 1"

${CLICKHOUSE_CLIENT} --query "SYSTEM DROP PARQUET METADATA CACHE"
${CLICKHOUSE_CLIENT} --query "SELECT count(), any(_headers['X-Method']) FROM url('http://127.0.0.1:${port}/data/*.parquet', 'Parquet', 'missing UInt64') SETTINGS ${settings}"
${CLICKHOUSE_CLIENT} --query "SELECT count(), any(_headers['X-Method']) FROM url('http://127.0.0.1:${port}/data/*.parquet', 'Parquet', 'missing UInt64') SETTINGS ${settings}"
