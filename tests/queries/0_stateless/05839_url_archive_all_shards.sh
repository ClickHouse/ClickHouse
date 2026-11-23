#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs a local HTTP server serving archives.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

# A `{a,b}` template names a set of shards, so the archive is read from every one of them. Such a path stays
# literal - no index page is listed - so each shard becomes one key of the read plan; keeping a single key let
# the Web object storage take the shards for failover options of that one key and silently drop the rows of the
# others. A `{a|b}` failover group cannot be combined with an archive read at all, which
# `05830_url_archive_custom_method` covers.

# Serves two archives, each holding `m.csv` with a single row. Both sockets are bound before the ports are
# published, and both ports go out in one write: printing from the serving threads lets the two `print` calls
# interleave, and `read` then sees one merged number and an empty line. One process keeps a single coproc,
# which a second `coproc` would warn about.
coproc SHARDS {
    exec python3 -u - <<'EOF'
import io
import re
import sys
import threading
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

def make_body(value):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("m.csv", f"{value}\n")
    return buffer.getvalue()

def make_handler(body):
    class Handler(BaseHTTPRequestHandler):
        def respond(self, with_data):
            if self.path.split("?")[0] != "/a.zip":
                self.send_error(404)
                return
            # The archive reader seeks, and a server that ignores `Range` makes the client log a warning,
            # which the test runner treats as a failure.
            data = body
            status = 200
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
                data = body[start : end + 1]
                status = 206
            self.send_response(status)
            self.send_header("Content-Type", "application/zip")
            self.send_header("Content-Length", str(len(data)))
            if status == 206:
                self.send_header("Content-Range", f"bytes {start}-{end}/{len(body)}")
            self.end_headers()
            if with_data:
                self.wfile.write(data)

        def do_GET(self):
            self.respond(True)

        def do_HEAD(self):
            self.respond(False)

        def log_message(self, *args):
            pass

    return Handler

def make_server(value):
    return ThreadingHTTPServer(("127.0.0.1", 0), make_handler(make_body(value)))

servers = [make_server(1), make_server(2)]
sys.stdout.write(f"{servers[0].server_address[1]}\n{servers[1].server_address[1]}\n")
sys.stdout.flush()

for server in servers:
    threading.Thread(target=server.serve_forever, daemon=True).start()

threading.Event().wait()
EOF
}
SERVER_PID=$SHARDS_PID
trap 'kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true' EXIT
read -r -t 30 PORT_A <&"${SHARDS[0]}"
read -r -t 30 PORT_B <&"${SHARDS[0]}"

# A port the read above did not deliver would be spliced into the URL below and fail far away, so fail here.
[[ "${PORT_A}" =~ ^[0-9]+$ && "${PORT_B}" =~ ^[0-9]+$ ]] || { echo "ports: '${PORT_A}' '${PORT_B}'"; exit 1; }

echo "--- a union of shards reads the archive from every shard"
${CLICKHOUSE_CLIENT} --query \
    "SELECT count(), sum(x) FROM url('http://{127.0.0.1:${PORT_A},127.0.0.1:${PORT_B}}/a.zip::m.csv', 'CSV', 'x UInt64')"
