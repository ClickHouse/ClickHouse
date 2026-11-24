#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs a local HTTP server serving archives with different contents per header value.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

# Request headers select what the server returns for the same URL, so the schema cache and the row-count
# cache have to tell two header sets apart. The cache identity is `<data source description>#<archive
# member>[#read_source_index][#headers=<hash>]`: without the header hash, a read with one header value
# answers from the cache another header value warmed, silently returning the other representation's row
# count. The mock serves a stable `Last-Modified` for every response, so a differing mtime cannot save it.

coproc ARCHIVE_HTTP {
    exec python3 -u - <<'EOF'
import io
import re
import sys
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

def make_archive(rows):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("m.tsv", "\n".join(str(i) for i in range(rows)) + "\n")
    return buffer.getvalue()

class Handler(BaseHTTPRequestHandler):
    def respond(self, body):
        data, status, start, end = body, 200, 0, len(body) - 1
        # The archive reader seeks, and a server that ignores `Range` makes the client log a warning, which
        # the test runner treats as a failure.
        range_header = self.headers.get("Range")
        if range_header:
            match = re.fullmatch(r"bytes=(\d+)-(\d*)", range_header)
            if not match:
                self.send_error(416)
                return
            start = int(match.group(1))
            end = int(match.group(2)) if match.group(2) else len(body) - 1
            end = min(end, len(body) - 1)
            if start > end:
                self.send_error(416)
                return
            data, status = body[start : end + 1], 206
        self.send_response(status)
        self.send_header("Content-Type", "application/zip")
        self.send_header("Content-Length", str(len(data)))
        # The same modification time for every response: only the header-selected content differs.
        self.send_header("Last-Modified", "Mon, 03 Nov 2025 10:00:00 GMT")
        if status == 206:
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(body)}")
        self.end_headers()
        if self.command == "GET":
            self.wfile.write(data)

    def do_GET(self):
        # The rows depend on the exact (name, value) pair, so the cache identity has to bound both: one row
        # for the name `X-Tenant` with any value but `b`, two rows for `X-Tenant=b`, and three rows for the
        # longer name `X-Tenantb` with the empty value - the pair that would collide with `X-Tenant=b` if
        # the identity hashed only the concatenation without the field boundary.
        if self.headers.get("X-Tenant") == "b":
            self.respond(make_archive(2))
        elif "X-Tenantb" in self.headers and self.headers["X-Tenantb"] == "":
            self.respond(make_archive(3))
        elif self.path.startswith("/dir%2Fa.zip"):
            # `%2F` is a literal of the object name with the encoding off, and one row lives there.
            self.respond(make_archive(1))
        elif self.path.startswith("/dir/a.zip"):
            # With the encoding on, `%2F` is decoded into a path separator, and another object lives there.
            self.respond(make_archive(2))
        else:
            self.respond(make_archive(1))

    do_HEAD = do_GET

    def log_message(self, *args):
        pass

server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
sys.stdout.write(f"{server.server_address[1]}\n")
sys.stdout.flush()
server.serve_forever()
EOF
}
SERVER_PID=$ARCHIVE_HTTP_PID
trap 'kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true' EXIT
read -r -t 30 PORT <&"${ARCHIVE_HTTP[0]}"

source="http://127.0.0.1:${PORT}/a.zip::m.tsv"

echo "--- a first read of one header value warms the caches with its own row count"
${CLICKHOUSE_CLIENT} --query \
    "SELECT count() FROM url('${source}', 'TSV', 'x UInt64', headers('X-Tenant'='a'))"

echo "--- and another header value does not reuse it"
${CLICKHOUSE_CLIENT} --query \
    "SELECT count() FROM url('${source}', 'TSV', 'x UInt64', headers('X-Tenant'='b'))"

echo "--- while the first value still answers with its own"
${CLICKHOUSE_CLIENT} --query \
    "SELECT count() FROM url('${source}', 'TSV', 'x UInt64', headers('X-Tenant'='a'))"

echo "--- and urlCluster separates them the same way"
${CLICKHOUSE_CLIENT} --query \
    "SELECT count() FROM urlCluster('test_shard_localhost', '${source}', 'TSV', 'x UInt64', headers('X-Tenant'='b'))"
${CLICKHOUSE_CLIENT} --query \
    "SELECT count() FROM urlCluster('test_shard_localhost', '${source}', 'TSV', 'x UInt64', headers('X-Tenant'='a'))"

echo "--- and a longer name with a shorter value is no other pair"
${CLICKHOUSE_CLIENT} --query \
    "SELECT count() FROM url('${source}', 'TSV', 'x UInt64', headers('X-Tenantb'=''))"

echo "--- and the URL encoding mode is part of the same identity"
# `%2F` stays a literal of the object name with the encoding off and names another object with it on,
# so warming one mode must not answer the other.
encoded_source="http://127.0.0.1:${PORT}/dir%2Fa.zip::m.tsv"
${CLICKHOUSE_CLIENT} --query \
    "SELECT count() FROM url('${encoded_source}', 'TSV', 'x UInt64') SETTINGS enable_url_encoding = 0"
${CLICKHOUSE_CLIENT} --query \
    "SELECT count() FROM url('${encoded_source}', 'TSV', 'x UInt64') SETTINGS enable_url_encoding = 1"
${CLICKHOUSE_CLIENT} --query \
    "SELECT count() FROM url('${encoded_source}', 'TSV', 'x UInt64') SETTINGS enable_url_encoding = 0"
