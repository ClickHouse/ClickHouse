#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs a local HTTP server serving an archive.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

# The access check of an archive read names the archive, not the member. A filtered grant written on the
# literal `archive::member` string matched before this version, because `:` is allowed in a `URL` grant
# subject, and it no longer matches: the object that is fetched is the archive.
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
user="url_archive_reader_${CLICKHOUSE_TEST_UNIQUE_NAME}"
function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${user}" || true
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
}
# Installed before the port is read, so a server that fails to start is still cleaned up.
trap cleanup EXIT
read -r -t 30 PORT <&"${ARCHIVE_HTTP[0]}"

${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE USER OR REPLACE ${user} IDENTIFIED WITH no_password;
    GRANT CREATE TEMPORARY TABLE ON *.* TO ${user};
    GRANT READ ON URL('http://127.0.0.1:${PORT}/a.zip::eod.csv') TO ${user}"

echo "--- a grant on the literal member string does not cover the archive read"
${CLICKHOUSE_CLIENT} --user="${user}" --query \
    "SELECT sum(c1) FROM url('http://127.0.0.1:${PORT}/a.zip :: eod.csv', 'CSV')" 2>&1 \
    | grep -o 'necessary to have the grant READ ON URL' | head -1

echo "--- while a grant on the archive does"
${CLICKHOUSE_CLIENT} --query "GRANT READ ON URL('http://127.0.0.1:${PORT}/a.zip') TO ${user}"
${CLICKHOUSE_CLIENT} --user="${user}" --query \
    "SELECT sum(c1) FROM url('http://127.0.0.1:${PORT}/a.zip :: eod.csv', 'CSV')"

echo "--- and revoking it brings the refusal back"
${CLICKHOUSE_CLIENT} --query "REVOKE READ ON URL('http://127.0.0.1:${PORT}/a.zip') FROM ${user}"
${CLICKHOUSE_CLIENT} --user="${user}" --query \
    "SELECT sum(c1) FROM url('http://127.0.0.1:${PORT}/a.zip :: eod.csv', 'CSV')" 2>&1 \
    | grep -o 'necessary to have the grant READ ON URL' | head -1

echo "--- a grant with spaces around the separator never matched"
${CLICKHOUSE_CLIENT} --query "GRANT READ ON URL('http://127.0.0.1:${PORT}/a.zip :: eod.csv') TO ${user}"
${CLICKHOUSE_CLIENT} --user="${user}" --query \
    "SELECT sum(c1) FROM url('http://127.0.0.1:${PORT}/a.zip :: eod.csv', 'CSV')" 2>&1 \
    | grep -o 'necessary to have the grant READ ON URL' | head -1

# `urlCluster` reads the same source through the same access subject, on the initiator and on the shard,
# so it needs its own arm: a check that only `url` reaches would still pass the cases above.
echo "--- urlCluster names the archive as well"
${CLICKHOUSE_CLIENT} --user="${user}" --query \
    "SELECT sum(c1) FROM urlCluster('test_shard_localhost', 'http://127.0.0.1:${PORT}/a.zip :: eod.csv', 'CSV')" 2>&1 \
    | grep -o 'necessary to have the grant READ ON URL' | head -1
${CLICKHOUSE_CLIENT} --query "GRANT READ ON URL('http://127.0.0.1:${PORT}/a.zip') TO ${user}"
${CLICKHOUSE_CLIENT} --user="${user}" --query \
    "SELECT sum(c1) FROM urlCluster('test_shard_localhost', 'http://127.0.0.1:${PORT}/a.zip :: eod.csv', 'CSV')"
