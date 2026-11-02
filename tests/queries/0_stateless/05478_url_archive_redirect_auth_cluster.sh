#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: reading ZIP archives requires minizip, disabled in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `urlCluster` worker probes the metadata of the URL the initiator pinned, and that URL is the target
# of a redirect. The target requires Basic authentication and the `Location` is absolute, so it carries
# no userinfo: the credentials have to survive both the hop and the serialization of the task, or the
# worker answers 401 instead of reading the archive.

coproc ARCHIVE_HTTP {
    exec python3 -u - <<'PY'
import base64
import io
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# The password is percent-encoded, so a round trip that decodes it would authenticate differently.
EXPECTED = "Basic " + base64.b64encode(b"user:p%25ass").decode()

data = io.BytesIO()
with zipfile.ZipFile(data, "w", compression=zipfile.ZIP_STORED) as archive:
    archive.writestr(zipfile.ZipInfo("data.json"), b'{"a":1}\n')
ARCHIVE = data.getvalue()


class Handler(BaseHTTPRequestHandler):
    def respond(self, head):
        if self.path == "/ready":
            self.send_response(200)
            self.send_header("Content-Length", "2")
            self.end_headers()
            if not head:
                self.wfile.write(b"ok")
            return
        if self.headers.get("Authorization") != EXPECTED:
            self.send_response(401)
            self.send_header("WWW-Authenticate", 'Basic realm="archive"')
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if self.path == "/archive.zip":
            self.send_response(302)
            self.send_header("Location", f"http://127.0.0.1:{self.server.server_port}/files/archive.zip")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if self.path != "/files/archive.zip":
            self.send_error(404)
            return
        begin, end = 0, len(ARCHIVE) - 1
        byte_range = self.headers.get("Range") if not head else None
        if byte_range is not None:
            first, last = byte_range.removeprefix("bytes=").split("-")
            if first:
                begin = int(first)
                if last:
                    end = min(int(last), end)
        self.send_response(206 if byte_range is not None else 200)
        self.send_header("Content-Length", str(end - begin + 1))
        self.send_header("Accept-Ranges", "bytes")
        if byte_range is not None:
            self.send_header("Content-Range", f"bytes {begin}-{end}/{len(ARCHIVE)}")
        self.end_headers()
        if not head:
            self.wfile.write(ARCHIVE[begin:end + 1])

    def do_HEAD(self):
        self.respond(head=True)

    def do_GET(self):
        self.respond(head=False)

    def log_message(self, *args):
        pass


server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
print(server.server_address[1], flush=True)
server.serve_forever()
PY
}
SERVER_PID=$ARCHIVE_HTTP_PID
trap 'kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true' EXIT
read -r -t 30 PORT <&"${ARCHIVE_HTTP[0]}"

# Readiness is an HTTP request, not a fixed delay.
${CLICKHOUSE_CLIENT} --query "SELECT * FROM url('http://127.0.0.1:${PORT}/ready', 'RawBLOB')"

echo "--- a worker reads the archive behind an authenticated absolute redirect"
${CLICKHOUSE_CLIENT} --allow_archive_path_syntax=1 --max_http_get_redirects=1 --http_max_tries=1 --query \
    "SELECT count(), sum(a) FROM urlCluster('test_cluster_one_shard_three_replicas_localhost', 'http://user:p%25ass@127.0.0.1:${PORT}/archive.zip::data.json', 'JSONEachRow', 'a UInt64')"
