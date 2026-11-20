#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs a local HTTP server serving an archive.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

# The nodes must not receive the inferred values of the `NOT OVERRIDABLE` keys of a named collection. The
# plain `StorageURLCluster` read keeps that by passing no columns, but the object-storage implementation that
# serves a wildcard or archive source infers them from the data before it builds the query for the nodes, so
# the structure would be sent anyway. That combination is rejected with an error that names the cause.
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
PLAIN = b"3\n"


class Handler(BaseHTTPRequestHandler):
    def respond(self, with_body):
        body = PLAIN if self.path.startswith("/plain") else ARCHIVE
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
        self.send_header("Content-Type", "application/octet-stream")
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
collection="url_cluster_archive_not_overridable_${CLICKHOUSE_TEST_UNIQUE_NAME}"
function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION IF EXISTS ${collection}" || true
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
}
# Installed before the port is read, so a server that fails to start is still cleaned up.
trap cleanup EXIT
read -r -t 30 PORT <&"${ARCHIVE_HTTP[0]}"

echo "--- a literal URL keeps the contract: the nodes infer the structure themselves"
${CLICKHOUSE_CLIENT} --query "
    CREATE OR REPLACE NAMED COLLECTION ${collection} AS
        url = 'http://127.0.0.1:${PORT}/plain.csv',
        format = 'CSV', structure = 'auto' NOT OVERRIDABLE"
${CLICKHOUSE_CLIENT} --query "SELECT sum(c1) FROM urlCluster('test_shard_localhost', ${collection})"

echo "--- an archive source rejects it instead of sending the inferred structure"
${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION ${collection}"
${CLICKHOUSE_CLIENT} --query "
    CREATE OR REPLACE NAMED COLLECTION ${collection} AS
        url = 'http://127.0.0.1:${PORT}/a.zip::eod.csv',
        format = 'CSV', structure = 'auto' NOT OVERRIDABLE"
${CLICKHOUSE_CLIENT} --query "SELECT sum(c1) FROM urlCluster('test_shard_localhost', ${collection})" 2>&1 \
    | grep -o 'A .NOT OVERRIDABLE. .structure. of a named collection is not supported for a wildcard or archive source' | head -1

# `format` is the other key the cluster path appends, so it needs its own arm: the nodes would receive the
# inferred format, which a `NOT OVERRIDABLE` key refuses there with an error that does not name the cause.
echo "--- an archive source rejects a NOT OVERRIDABLE format as well"
${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION ${collection}"
${CLICKHOUSE_CLIENT} --query "
    CREATE OR REPLACE NAMED COLLECTION ${collection} AS
        url = 'http://127.0.0.1:${PORT}/a.zip::eod.csv',
        format = 'auto' NOT OVERRIDABLE, structure = 'c1 String'"
${CLICKHOUSE_CLIENT} --query "SELECT sum(c1) FROM urlCluster('test_shard_localhost', ${collection})" 2>&1 \
    | grep -o 'A .NOT OVERRIDABLE. .format. of a named collection is not supported for a wildcard or archive source' | head -1

echo "--- and so does a wildcard source"
${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION ${collection}"
${CLICKHOUSE_CLIENT} --query "
    CREATE OR REPLACE NAMED COLLECTION ${collection} AS
        url = 'http://127.0.0.1:${PORT}/plain*.csv',
        format = 'auto' NOT OVERRIDABLE, structure = 'c1 String'"
${CLICKHOUSE_CLIENT} --query "SELECT sum(c1) FROM urlCluster('test_shard_localhost', ${collection})" 2>&1 \
    | grep -o 'A .NOT OVERRIDABLE. .format. of a named collection is not supported for a wildcard or archive source' | head -1

echo "--- while the non-cluster read of the same source infers it locally"
${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION ${collection}"
${CLICKHOUSE_CLIENT} --query "
    CREATE OR REPLACE NAMED COLLECTION ${collection} AS
        url = 'http://127.0.0.1:${PORT}/a.zip::eod.csv',
        format = 'CSV', structure = 'auto' NOT OVERRIDABLE"
${CLICKHOUSE_CLIENT} --query "SELECT sum(c1) FROM url(${collection})"
