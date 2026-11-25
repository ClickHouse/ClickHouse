#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs a local HTTP server serving archives and redirects.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

# Two users check the two grants an ad-hoc archive read needs, so removing either check fails the test:
#   - `no_source` holds only the archive grant, and is refused before any HTTP request by the
#     whole-source check the redirect-following reader demands (`url` and `urlCluster` alike);
#   - `reader` holds the whole-source grant, and is refused again as soon as the archive grant on the
#     role is taken away, so the read is not answered by the whole-source grant alone.
# A grant written on the literal `archive::member` string never matches the archive, because `::` is not
# part of the fetched URL, and a redirect makes a granted archive read an ungranted one: both stay
# refused by the whole-source check running before any request.

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
        archive.writestr(member, "\n".join(str(i) for i in range(value)) + "\n")
    return buffer.getvalue()

# `allowed.zip` redirects to `private.zip`, so a reader that follows redirects without another check
# would read the second archive under the grant of the first one.
ALLOWED = make_archive("eod.csv", 1)
PRIVATE = make_archive("eod.csv", 99)

class Handler(BaseHTTPRequestHandler):
    def respond(self, body):
        data, status, start, end = body, 200, 0, len(body) - 1
        # The archive reader seeks, and a server that ignores `Range` makes the client log a warning,
        # which the test runner treats as a failure.
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
        self.send_header("Last-Modified", "Mon, 03 Nov 2015 07:28:00 GMT")
        if status == 206:
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(body)}")
        self.end_headers()
        if self.command == "GET":
            self.wfile.write(data)

    def do_GET(self):
        path = self.path.split("?")[0]
        port = self.server.server_address[1]
        if path == "/redirect/":
            index = f'<a href="http://127.0.0.1:{port}/redirect/allowed.zip">allowed.zip</a>\n'.encode()
            self.respond_redirect_index(index)
        elif path == "/redirect/allowed.zip":
            self.send_response(302)
            self.send_header("Location", f"http://127.0.0.1:{port}/redirect/private.zip")
            self.send_header("Content-Length", "0")
            self.end_headers()
        elif path == "/redirect/private.zip":
            self.respond(PRIVATE)
        else:
            self.send_error(404)

    def respond_redirect_index(self, index):
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.send_header("Content-Length", str(len(index)))
        self.end_headers()
        if self.command == "GET":
            self.wfile.write(index)

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
role="url_archive_reader_role_${CLICKHOUSE_TEST_UNIQUE_NAME}"
no_source="url_archive_reader_no_source_${CLICKHOUSE_TEST_UNIQUE_NAME}"
reader="url_archive_reader_${CLICKHOUSE_TEST_UNIQUE_NAME}"
function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${no_source}" || true
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${reader}" || true
    ${CLICKHOUSE_CLIENT} --query "DROP ROLE IF EXISTS ${role}" || true
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
}
# Installed before the port is read, so a server that fails to start is still cleaned up.
trap cleanup EXIT
read -r -t 30 PORT <&"${ARCHIVE_HTTP[0]}"

${CLICKHOUSE_CLIENT} --multiquery --query "
    DROP ROLE IF EXISTS ${role};
    CREATE ROLE ${role};
    CREATE USER OR REPLACE ${no_source} IDENTIFIED WITH no_password;
    CREATE USER OR REPLACE ${reader} IDENTIFIED WITH no_password;
    GRANT CREATE TEMPORARY TABLE ON *.* TO ${no_source}, ${reader};
    GRANT READ ON URL TO ${reader};
    GRANT READ ON URL('http://127.0.0.1:${PORT}/redirect/allowed.zip') TO ${role};
    GRANT ${role} TO ${no_source}"

source="http://127.0.0.1:${PORT}/redirect/allowed.zip :: eod.csv"

echo "--- an archive grant without the whole-source grant is refused, with no request sent"
${CLICKHOUSE_CLIENT} --user="${no_source}" --query \
    "SELECT sum(c1) FROM url('${source}', 'CSV')" 2>&1 \
    | grep -o 'necessary to have the grant READ ON URL' | head -1

echo "--- and so is its schema inference"
${CLICKHOUSE_CLIENT} --user="${no_source}" --query \
    "DESCRIBE TABLE url('${source}', 'CSV')" 2>&1 \
    | grep -o 'necessary to have the grant READ ON URL' | head -1

echo "--- and the clustered read of the same source"
${CLICKHOUSE_CLIENT} --user="${no_source}" --query \
    "SELECT sum(c1) FROM urlCluster('test_shard_localhost', '${source}', 'CSV')" 2>&1 \
    | grep -o 'necessary to have the grant READ ON URL' | head -1

echo "--- while the whole-source grant plus the archive grant reads through the redirect"
${CLICKHOUSE_CLIENT} --query "GRANT ${role} TO ${reader}"
${CLICKHOUSE_CLIENT} --user="${reader}" --query \
    "SELECT sum(c1) FROM url('${source}', 'CSV') SETTINGS max_http_get_redirects = 1"

echo "--- and taking the archive grant back brings the refusal back, before any request"
${CLICKHOUSE_CLIENT} --query "REVOKE ${role} FROM ${reader}"
${CLICKHOUSE_CLIENT} --user="${reader}" --query \
    "SELECT sum(c1) FROM url('${source}', 'CSV')" 2>&1 \
    | grep -o 'necessary to have the grant READ ON URL' | head -1

echo "--- a grant on the literal member string never covered the archive"
${CLICKHOUSE_CLIENT} --query "GRANT READ ON URL('http://127.0.0.1:${PORT}/redirect/allowed.zip :: eod.csv') TO ${reader}"
${CLICKHOUSE_CLIENT} --user="${reader}" --query \
    "SELECT sum(c1) FROM url('${source}', 'CSV')" 2>&1 \
    | grep -o 'necessary to have the grant READ ON URL' | head -1

echo "--- and the clustered read names the archive as well"
${CLICKHOUSE_CLIENT} --user="${reader}" --query \
    "SELECT sum(c1) FROM urlCluster('test_shard_localhost', '${source}', 'CSV')" 2>&1 \
    | grep -o 'necessary to have the grant READ ON URL' | head -1

${CLICKHOUSE_CLIENT} --multiquery --query "
    GRANT READ ON URL('http://127.0.0.1:${PORT}/redirect/allowed.zip') TO ${role};
    GRANT ${role} TO ${reader}"
${CLICKHOUSE_CLIENT} --user="${reader}" --query \
    "SELECT sum(c1) FROM urlCluster('test_shard_localhost', '${source}', 'CSV') SETTINGS max_http_get_redirects = 1"

echo "--- a table of a dynamic URL database is not a stored definition for the whole-source grant"
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${CLICKHOUSE_DATABASE}_url_database ENGINE = URL('http://127.0.0.1:${PORT}/redirect')"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}_url_database.* TO ${no_source}, ${reader}"
url_database_table="${CLICKHOUSE_DATABASE}_url_database.\`http://127.0.0.1:${PORT}/redirect/allowed.zip::eod.csv\`"
${CLICKHOUSE_CLIENT} --user="${no_source}" --query \
    "DESCRIBE ${url_database_table} SETTINGS max_http_get_redirects = 1" 2>&1 \
    | grep -o 'necessary to have the grant READ ON URL' | head -1
${CLICKHOUSE_CLIENT} --user="${no_source}" --query \
    "SELECT sum(c1) FROM ${url_database_table} SETTINGS max_http_get_redirects = 1" 2>&1 \
    | grep -o 'necessary to have the grant READ ON URL' | head -1
${CLICKHOUSE_CLIENT} --user="${reader}" --query \
    "SELECT sum(c1) FROM ${url_database_table} SETTINGS max_http_get_redirects = 1"

echo "--- a failover group in the name of a dynamic database table is refused like a fresh url() call"
failover_url="http://127.0.0.1:${PORT}/redirect/{a|b}/**/p.csv"
${CLICKHOUSE_CLIENT} --user="${reader}" --query \
    "SELECT 1 FROM url('${failover_url}', 'CSV') SETTINGS allow_url_wildcard_from_index_pages = 1" 2>&1 \
    | grep -o "Failover patterns ('|') in the path are not supported" | head -1
failover_table="${CLICKHOUSE_DATABASE}_url_database.\`${failover_url}\`"
${CLICKHOUSE_CLIENT} --user="${reader}" --query \
    "SELECT 1 FROM ${failover_table} SETTINGS allow_url_wildcard_from_index_pages = 1" 2>&1 \
    | grep -o "Failover patterns ('|') in the path are not supported" | head -1

${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${CLICKHOUSE_DATABASE}_url_database"
