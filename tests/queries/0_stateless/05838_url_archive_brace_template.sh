#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs a local HTTP server serving an index page and two archives.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

# A `{...}` union template in the *path* of an archive URL is not expanded locally, unlike the same template
# in a non-archive `url` source: it lists the HTTP index page, so it requires
# `allow_url_wildcard_from_index_pages`. Both directions are pinned here.
coproc ARCHIVE_HTTP {
    exec python3 -u - <<'EOF'
import io
import re
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def make_archive(value):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("value.tsv", f"{value}\n")
    return buffer.getvalue()


ARCHIVES = {"/data/arch0.zip": make_archive(11), "/data/arch1.zip": make_archive(22)}
INDEX = b'<a href="arch0.zip">arch0.zip</a>\n<a href="arch1.zip">arch1.zip</a>\n'


class Handler(BaseHTTPRequestHandler):
    def respond(self, with_body):
        path = self.path.split("?")[0]
        if path == "/data/":
            body = INDEX
            content_type = "text/html"
        elif path in ARCHIVES:
            body = ARCHIVES[path]
            content_type = "application/zip"
        else:
            self.send_error(404)
            return
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
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(body)}")
            body = body[start : end + 1]
        else:
            self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()
        if with_body:
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                # A seek closes the response before its whole body is consumed.
                pass

    def do_GET(self):
        self.respond(True)

    def do_HEAD(self):
        self.respond(False)

    def log_message(self, *args):
        pass


server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
print(server.server_address[1], flush=True)
server.serve_forever()
EOF
}
SERVER_PID=$ARCHIVE_HTTP_PID
# Installed before the port is read, so a server that fails to start is still cleaned up.
trap 'kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true' EXIT
read -r -t 30 PORT <&"${ARCHIVE_HTTP[0]}"

source="http://127.0.0.1:${PORT}/data/{arch0,arch1}.zip :: value.tsv"

echo "--- a brace template in the archive path lists the index page"
${CLICKHOUSE_CLIENT} --allow_url_wildcard_from_index_pages=1 --query \
    "SELECT sum(x) FROM url('${source}', 'TSV', 'x UInt64')"

echo "--- and without the setting it is refused"
${CLICKHOUSE_CLIENT} --query "SELECT sum(x) FROM url('${source}', 'TSV', 'x UInt64')" 2>&1 \
    | grep -o 'Wildcard expansion for .url. from HTTP index pages is experimental' | head -1

# `urlCluster` has its own check point, so it needs its own arm: a regression that dropped it would silently
# list the index page.
echo "--- urlCluster refuses it as well"
${CLICKHOUSE_CLIENT} --query "SELECT sum(x) FROM urlCluster('test_shard_localhost', '${source}', 'TSV', 'x UInt64')" 2>&1 \
    | grep -o 'Wildcard expansion for .urlCluster. from HTTP index pages is experimental' | head -1

echo "--- and reads it with the setting"
${CLICKHOUSE_CLIENT} --allow_url_wildcard_from_index_pages=1 --query \
    "SELECT sum(x) FROM urlCluster('test_shard_localhost', '${source}', 'TSV', 'x UInt64')" 
