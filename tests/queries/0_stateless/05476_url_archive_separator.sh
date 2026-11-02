#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

# `::` is the archive separator only where it can be one: not in a query or fragment of a URL whose path
# is a plain glob, and not inside the bracketed host of an IPv6 literal. Both are ordinary URLs, and
# `allow_archive_path_syntax` is on by default, so getting them wrong breaks queries that never mention
# an archive.

# A local mock serves the two files the brace pattern expands to, so a literal read is observable.
# It publishes its port only after binding, so readiness needs no polling.
coproc ARCHIVE_HTTP {
    exec python3 -u - <<'EOF'
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

FILES = {"/data1.csv": b"1\n", "/data2.csv": b"2\n"}


class Handler(BaseHTTPRequestHandler):
    def respond(self, with_body):
        body = FILES.get(self.path.split("?")[0])
        if body is None:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/csv")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if with_body:
            self.wfile.write(body)

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

echo "--- a plain glob path with '::' in its query stays a literal URL"
${CLICKHOUSE_CLIENT} --allow_archive_path_syntax=1 --query \
    "SELECT sum(x) FROM url('http://127.0.0.1:${PORT}/data{1,2}.csv?x=::1', 'CSV', 'x UInt64')"
echo "--- and so does one with '::' in its fragment"
${CLICKHOUSE_CLIENT} --allow_archive_path_syntax=1 --query \
    "SELECT sum(x) FROM url('http://127.0.0.1:${PORT}/data{1,2}.csv#x=::1', 'CSV', 'x UInt64')"

# The `::` of an IPv6 literal in the authority is not the separator either. The archive path is reported
# by the write rejection, so the message also shows where the string was split: a split at the IPv6 `::`
# would report `http://[` instead of `a.zip`.
check_archive_path()
{
    local url="$1"
    local expected="$2"
    local error
    error=$(${CLICKHOUSE_CLIENT} --allow_archive_path_syntax=1 --query \
        "INSERT INTO FUNCTION url('${url}', 'CSV', 'x UInt64') VALUES (1)" 2>&1 || true)
    if echo "$error" | grep -q "Path '${expected}' contains archive. Write into archive is not supported"; then
        echo "archive path ${expected}"
    else
        echo "unexpected result for ${url}: ${error}"
    fi
}

echo "--- a bracketed host keeps its '::'"
check_archive_path 'http://[::1]:1/a.zip::value.tsv' 'a.zip'
check_archive_path 'http://user:pass@[::1]:1/a.zip::value.tsv' 'a.zip'
check_archive_path 'http://[::1]:1/a.zip :: value.tsv' 'a.zip'
echo "--- and so does every bracketed host of a host template"
check_archive_path 'http://{[::1]:1|[::2]:1}/a.zip::value.tsv' 'a.zip'
check_archive_path 'http://{[::1]:1|127.0.0.1:1}/a.zip::value.tsv' 'a.zip'
echo "--- a '::' in the userinfo is not the separator"
check_archive_path 'http://user:p::q@127.0.0.1:1/a.zip::value.tsv' 'a.zip'
echo "--- and without a bracketed host the first '::' is still the separator"
check_archive_path 'http://127.0.0.1:1/a.zip::value.tsv' 'a.zip'

echo "--- a path failover alternative that carries its own scheme is rejected"
${CLICKHOUSE_CLIENT} --allow_archive_path_syntax=1 --query \
    "SELECT count() FROM url('http://127.0.0.1:1/a.zip|http://127.0.0.1:2/a.zip :: value.tsv', 'CSV', 'x UInt64')" \
    2>&1 | grep -o 'Failover alternative .* cannot be honoured' | head -1

echo "--- the URL table engine does not honour archive syntax"
for engine_url in 'http://127.0.0.1:1/a.zip::value.tsv' 'http://127.0.0.1:1/*.zip::value.tsv'; do
    ${CLICKHOUSE_CLIENT} --allow_archive_path_syntax=1 --query \
        "CREATE TABLE url_engine_archive (x UInt64) ENGINE = URL('${engine_url}', 'CSV')" \
        2>&1 | grep -o 'Archive path syntax is not supported by the .URL. table engine' | head -1
done

echo "--- and a definition whose setting differs from the server default is rejected"
# The wildcard shape is the one the engine would read through the object storage, so it is the one whose
# interpretation has to be settled at creation time.
${CLICKHOUSE_CLIENT} --allow_archive_path_syntax=0 --allow_url_wildcard_from_index_pages=1 --query \
    "CREATE TABLE url_engine_mismatch (x UInt64) ENGINE = URL('http://127.0.0.1:1/*.zip::value.tsv', 'CSV')" \
    2>&1 | grep -o 'differs from the server default' | head -1
