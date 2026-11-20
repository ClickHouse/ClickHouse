#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs a local HTTP server serving an archive.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

# The virtual columns of an archive member are documented as: `_path` is
# `<namespace>/<archive path>::<member path>`, `_file` is the member path, `_size` is the member's size as
# stored inside the archive, `_time` is the last modified time of the outer archive, and `_headers` are the
# response headers of the metadata probe of the outer archive. `_time` and `_headers` come from two different
# mechanisms, so a regression in either would be silent without this test.
coproc ARCHIVE_HTTP {
    exec python3 -u - <<'EOF'
import io
import re
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BUFFER = io.BytesIO()
with zipfile.ZipFile(BUFFER, "w") as archive:
    archive.writestr("eod.csv", "1\n2\n")
ARCHIVE = BUFFER.getvalue()
LAST_MODIFIED = "Wed, 21 Oct 2015 07:28:00 GMT"


class Handler(BaseHTTPRequestHandler):
    def respond(self, with_body):
        body = ARCHIVE
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
        self.send_header("Content-Type", "application/zip")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Last-Modified", LAST_MODIFIED)
        # Only the metadata probe carries it, so the assertion pins that `_headers` comes from the probe
        # response rather than from whichever request happened to be answered last.
        if not with_body:
            self.send_header("X-Test-Header", "1")
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
trap 'kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true' EXIT
read -r -t 30 PORT <&"${ARCHIVE_HTTP[0]}"

# The port is part of the namespace, and `_time` is rendered in the session time zone, so both are pinned
# to a stable form instead of being printed as they are.
show_virtual_columns()
{
    ${CLICKHOUSE_CLIENT} --query "
        SELECT DISTINCT
            _path = '127.0.0.1:${PORT}/a.zip::eod.csv' AS path_is_namespace_archive_member,
            _file,
            _size,
            formatDateTime(_time, '%F %T', 'UTC') AS time_utc,
            _headers['X-Test-Header'] AS probe_header
        FROM url('http://127.0.0.1:${PORT}/a.zip :: $1', 'CSV', 'x UInt64')"
}

echo "--- the virtual columns of an archive member"
show_virtual_columns 'eod.csv'

echo "--- and a member glob reports the same values"
show_virtual_columns '*.csv'

# The clustered read of the same source goes through the object-storage implementation on the initiator, so
# it has to expose the same virtual columns as the worker that reads the bytes.
echo "--- and urlCluster exposes them as well"
${CLICKHOUSE_CLIENT} --query "
    SELECT DISTINCT
        _path = '127.0.0.1:${PORT}/a.zip::eod.csv' AS path_is_namespace_archive_member,
        _file,
        _size,
        formatDateTime(_time, '%F %T', 'UTC') AS time_utc,
        _headers['X-Test-Header'] AS probe_header
    FROM urlCluster('test_shard_localhost', 'http://127.0.0.1:${PORT}/a.zip :: eod.csv', 'CSV', 'x UInt64')"

# A member-level predicate is what lets the read skip an archive that does not hold the member, so it has to
# compare against the virtual path the reader reports: `<namespace>/<archive>::<member>` for `_path`, and the
# member name without the archive for `_file`.
echo "--- and a member-level predicate selects the same member"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM url('http://127.0.0.1:${PORT}/a.zip :: eod.csv', 'CSV', 'x UInt64')
    WHERE _path = '127.0.0.1:${PORT}/a.zip::eod.csv' AND _file = 'eod.csv'"

echo "--- while another member name matches nothing"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM url('http://127.0.0.1:${PORT}/a.zip :: eod.csv', 'CSV', 'x UInt64')
    WHERE _file = 'other.csv'"
