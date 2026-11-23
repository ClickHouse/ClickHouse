#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs a local HTTP server serving an archive.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

# A read task names the archive and the member as one `<archive>::<member>` string, and the worker splits it
# at the first `::`. A listed object key that contains `::` therefore cannot be dispatched: the worker would
# take the part of the key after the first separator as the member and read a different object, which returns
# the wrong rows instead of an error. Such a key is only listed from an absolute href - a relative one is
# dropped by the index page parser - so the fixture links the archive by its full URL.
#
# `url` reads the same source on the initiator, where the key and the member are known separately.

coproc ARCHIVE_HTTP {
    exec python3 -u - <<'EOF'
import io
import re
import sys
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

def make_archive(member, value):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(member, f"{value}\n")
    return buffer.getvalue()

# The listed object key itself contains the separator; the second object is the one a wrong split reads.
LISTED = make_archive("m.csv", 2)
UNLISTED = make_archive("b.zip::m.csv", 1)
OBJECTS = {"/archives/a.zip::b.zip": LISTED, "/archives/a.zip": UNLISTED}

class Handler(BaseHTTPRequestHandler):
    def respond(self, body, content_type):
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
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Accept-Ranges", "bytes")
        if status == 206:
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(body)}")
        self.end_headers()
        if self.command == "GET":
            self.wfile.write(data)

    def do_GET(self):
        path = self.path.split("?")[0]
        port = self.server.server_address[1]
        if path == "/archives/":
            index = f'<a href="http://127.0.0.1:{port}/archives/a.zip::b.zip">a.zip::b.zip</a>\n'.encode()
            self.respond(index, "text/html")
        elif path in OBJECTS:
            self.respond(OBJECTS[path], "application/zip")
        else:
            self.send_error(404)

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

SOURCE="http://127.0.0.1:${PORT}/archives/*.zip :: *.csv"

echo "--- url reads the listed object, whose key contains the separator"
${CLICKHOUSE_CLIENT} --allow_url_wildcard_from_index_pages=1 --query \
    "SELECT count(), sum(x) FROM url('${SOURCE}', 'CSV', 'x UInt64')"

echo "--- a per-member task cannot name it, so urlCluster refuses instead of reading another object"
${CLICKHOUSE_CLIENT} --allow_url_wildcard_from_index_pages=1 --query \
    "SELECT count() FROM urlCluster('test_shard_localhost', '${SOURCE}', 'CSV', 'x UInt64')
     SETTINGS cluster_function_process_archive_on_multiple_nodes = 1" 2>&1 \
    | grep -o "contains '::', which is the archive-member separator" | head -1

echo "--- and so does a whole-archive task, which the worker cannot tell from a per-member one"
${CLICKHOUSE_CLIENT} --allow_url_wildcard_from_index_pages=1 --query \
    "SELECT count() FROM urlCluster('test_shard_localhost', '${SOURCE}', 'CSV', 'x UInt64')
     SETTINGS cluster_function_process_archive_on_multiple_nodes = 0" 2>&1 \
    | grep -o "contains '::', which is the archive-member separator" | head -1
