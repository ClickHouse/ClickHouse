#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: an S3 Tables catalog needs the USE_AVRO, USE_SSL and USE_AWS_S3 build options, and python3.

# A `DataLakeCatalog` database with `catalog_type = 's3tables'` signs every catalog request with AWS
# SigV4 instead of sending a bearer token. The signature has to cover the headers the catalog layer
# adds to the request (`Content-Type`, `X-Iceberg-Access-Delegation`) and the JSON body of a POST,
# as https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html requires.
# The stub below answers 401 unless the request carries an `AWS4-HMAC-SHA256` authorization header
# for the `s3tables` service whose `SignedHeaders` lists the header expected on that endpoint and
# whose `x-amz-content-sha256` is the SHA-256 of the body it actually read. So each result below is
# reachable only through a request that was signed with those headers and that body.

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_s3tables"
# The stub binds a free port itself and reports it through a FIFO, so no other test can take the
# port meanwhile and no polling is needed: the socket is listening before the port is readable.
FIFO="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.port"
rm -f "$FIFO"
mkfifo "$FIFO"

python3 - "$FIFO" <<'EOF' &
import hashlib
import re
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

AUTH = re.compile(r"^AWS4-HMAC-SHA256 Credential=[^/]+/[0-9]{8}/us-east-1/s3tables/aws4_request,"
                  r" SignedHeaders=([a-z0-9;.*_-]+), Signature=[0-9a-f]{64}$")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def read_body(self):
        if "chunked" in (self.headers.get("Transfer-Encoding") or "").lower():
            out = b""
            while True:
                size = int(self.rfile.readline().split(b";")[0].strip() or b"0", 16)
                if not size:
                    self.rfile.readline()
                    return out
                out += self.rfile.read(size)
                self.rfile.read(2)
        return self.rfile.read(int(self.headers.get("Content-Length") or 0))

    def reply(self):
        path = self.path.partition("?")[0]
        body = self.read_body()
        if path.endswith("/config"):
            need, status, out = "x-amz-date", 200, b'{"defaults":{},"overrides":{}}'
        elif path.endswith("/tables/tbl"):
            # An empty body is how the Iceberg REST catalog reports a table that does not exist.
            need, status, out = "x-iceberg-access-delegation", 200, b""
        elif self.command == "POST":
            # Only the namespace creation posts; 409 means "another client created it", i.e. accepted.
            need, status, out = "content-type", 409, b"{}"
        elif path.endswith("/namespaces/nspace"):
            need, status, out = "x-amz-date", 404, b"{}"
        else:
            need, status, out = "x-amz-date", 200, b'{"namespaces":[],"identifiers":[]}'
        signed = AUTH.match(self.headers.get("Authorization") or "")
        if (not signed or need not in signed.group(1).split(";")
                or self.headers.get("x-amz-content-sha256") != hashlib.sha256(body).hexdigest()):
            status, out = 401, b"{}"
        self.send_response(status)
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)

    do_GET = do_POST = do_DELETE = reply

    def log_message(self, *args):
        pass


server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
with open(sys.argv[1], "w") as f:
    f.write(str(server.server_address[1]) + "\n")
server.serve_forever()
EOF
STUB_PID=$!
trap '${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB}"; kill ${STUB_PID} 2>/dev/null; rm -f "${FIFO}"' EXIT
read -r PORT < "$FIFO"

# Only the error code name is kept: the messages carry the database name and the endpoint.
err_code() {
    ${CLICKHOUSE_CLIENT} --query "$1" 2>&1 | grep -oE '\([A-Z][A-Z_0-9]+\)' | tail -1
}

# The catalog is built eagerly by CREATE DATABASE, which signs `GET /v1/config?warehouse=wh` with no
# extra header and no payload. The database is there only if the stub accepted that signature.
${CLICKHOUSE_CLIENT} --query "
    SET allow_database_iceberg = 1;
    CREATE DATABASE ${DB} ENGINE = DataLakeCatalog('http://127.0.0.1:${PORT}/v1')
    SETTINGS catalog_type = 's3tables', region = 'us-east-1', warehouse = 'wh',
        aws_access_key_id = 'key_${CLICKHOUSE_DATABASE}', aws_secret_access_key = 'sec_${CLICKHOUSE_DATABASE}'"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.databases WHERE name = '${DB}'"

# A table lookup asks for vended credentials, which adds `X-Iceberg-Access-Delegation` to the request.
# The stub answers the empty body that means "no such table" only if that header was signed as well.
err_code "SELECT count() FROM ${DB}.\`nspace.tbl\`"

# Creating a table without a location makes the catalog create the namespace: a GET that is answered
# 404, then a POST whose JSON body is part of the signature. The stub accepts the POST only when
# `x-amz-content-sha256` matches the body it read, and the namespace still has no location afterwards.
err_code "CREATE TABLE ${DB}.\`nspace.tbl\` (x UInt8)"
