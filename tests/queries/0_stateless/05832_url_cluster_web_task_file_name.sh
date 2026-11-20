#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs a local HTTP server serving an index page.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

# A distributed Web task carries only the object key, not the `derive_file_name_from_url_path` bit the
# initiator recorded for it, because the cluster-function protocol has no field for it. The worker has to
# restore that from the storage type: for a Web source a `?` or a `#` in the key is never part of the object
# name, only the query or the fragment of the request. Getting it wrong makes `_file` report the query and
# makes `compression_method = 'auto'` miss the suffix of the file.
#
# The mock lists one plain and one gzip-compressed file, both behind a query, so a single query covers both
# consequences of the lost bit: the value of `_file` and the decompression of `part2.tsv.gz`.
coproc INDEX_HTTP {
    exec python3 -u - <<'EOF'
import gzip
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

INDEX = (
    b'<a href="part1.tsv?download=1">part1.tsv?download=1</a>\n'
    b'<a href="part2.tsv.gz?download=1">part2.tsv.gz?download=1</a>\n'
)
FILES = {
    "/data/query_override/part1.tsv": b"6\n",
    "/data/query_override/part2.tsv.gz": gzip.compress(b"7\n"),
}


class Handler(BaseHTTPRequestHandler):
    def respond(self, with_body):
        path, _, query = self.path.partition("?")
        if path == "/data/query_override/":
            body = INDEX
        elif path in FILES and query == "download=1":
            body = FILES[path]
        else:
            self.send_error(404)
            return
        # The gzip reader seeks, and a server that ignores `Range` makes the client log a warning, which
        # the test runner treats as a failure.
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
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(body)}")
            body = body[start : end + 1]
        else:
            self.send_response(status)
        self.send_header("Content-Type", "text/html" if path.endswith("/") else "application/octet-stream")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Accept-Ranges", "bytes")
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
SERVER_PID=$INDEX_HTTP_PID
trap 'kill "$SERVER_PID" 2>/dev/null || true; wait "$SERVER_PID" 2>/dev/null || true' EXIT
read -r -t 30 PORT <&"${INDEX_HTTP[0]}"

echo "--- the file name of a distributed Web task is the visible one"
${CLICKHOUSE_CLIENT} --allow_url_wildcard_from_index_pages=1 --query \
    "SELECT _file, sum(x) FROM urlCluster('test_shard_localhost', 'http://127.0.0.1:${PORT}/data/query_override/part*?x=1', 'TSV', 'x UInt64', 'auto') GROUP BY _file ORDER BY _file"

echo "--- and the compression is inferred from it"
${CLICKHOUSE_CLIENT} --allow_url_wildcard_from_index_pages=1 --query \
    "SELECT sum(x) FROM urlCluster('test_shard_localhost', 'http://127.0.0.1:${PORT}/data/query_override/part*?x=1', 'TSV', 'x UInt64', 'auto') WHERE _file = 'part2.tsv.gz'"

echo "--- the same read without a cluster reports the same names"
${CLICKHOUSE_CLIENT} --allow_url_wildcard_from_index_pages=1 --query \
    "SELECT _file, sum(x) FROM url('http://127.0.0.1:${PORT}/data/query_override/part*?x=1', 'TSV', 'x UInt64', 'auto') GROUP BY _file ORDER BY _file"

# `_headers` is exposed by every shape: a literal URL takes it from the data `GET`, while a wildcard or
# archive source goes through the object-storage implementation, which takes it from its own probe or `GET`.
echo "--- a literal URL keeps the response headers"
${CLICKHOUSE_CLIENT} --query \
    "SELECT _headers['X-Test-Header'], sum(x) FROM urlCluster('test_shard_localhost', 'http://127.0.0.1:${PORT}/data/query_override/part1.tsv?download=1', 'TSV', 'x UInt64') GROUP BY 1"
