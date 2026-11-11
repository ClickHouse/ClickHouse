#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: an S3 Tables catalog needs the USE_AVRO, USE_SSL and USE_AWS_S3 build options, and python3.

# A `DataLakeCatalog` database with `catalog_type = 's3tables'` signs every catalog request with AWS
# SigV4 instead of a bearer token. The stub below recomputes the SigV4 HMAC from the test credentials
# and the request it really received, and answers 401 unless it equals the signature that was sent. So
# every result below needs a request whose method, URI, signed header values (`Content-Type`,
# `X-Iceberg-Access-Delegation`) and chunked JSON body were all signed correctly.

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=fatal

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_s3tables"
FIFO="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.port"
rm -f "$FIFO"
mkfifo "$FIFO"

python3 - "$FIFO" "key_${CLICKHOUSE_DATABASE}" "sec_${CLICKHOUSE_DATABASE}" <<'EOF' &
import hashlib, hmac, re, sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

FIFO, KEY, SECRET, REGION, SERVICE = sys.argv[1:4] + ["us-east-1", "s3tables"]
AUTH = re.compile(r"^AWS4-HMAC-SHA256 Credential=([^/]+)/([0-9]{8})/%s/%s/aws4_request,"
                  r" SignedHeaders=([a-z0-9;.*_-]+), Signature=([0-9a-f]{64})$" % (REGION, SERVICE))

def mac(key, msg): return hmac.new(key, msg.encode(), hashlib.sha256).digest()

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def signature(self, names, date, payload_hash):
        # https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
        headers = "".join("%s:%s\n" % (n, " ".join(self.headers[n].split())) for n in names)
        path, _, query = self.path.partition("?")
        request = "\n".join([self.command, path, "&".join(sorted(filter(None, query.split("&")))),
                             headers, ";".join(names), payload_hash])
        to_sign = "\n".join(["AWS4-HMAC-SHA256", self.headers["x-amz-date"],
                             "/".join([date, REGION, SERVICE, "aws4_request"]),
                             hashlib.sha256(request.encode()).hexdigest()])
        key = mac(("AWS4" + SECRET).encode(), date)
        for part in (REGION, SERVICE, "aws4_request"):
            key = mac(key, part)
        return hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()

    def accepted(self, need, body):
        signed = AUTH.match(self.headers.get("Authorization") or "")
        if not signed:
            return False
        key_id, date, names, sent = signed.groups()
        names = names.split(";")
        payload_hash = self.headers.get("x-amz-content-sha256")
        if (key_id != KEY or need not in names or "x-amz-date" not in names
                or payload_hash != hashlib.sha256(body).hexdigest()
                or any(self.headers.get(n) is None for n in names)):
            return False
        return hmac.compare_digest(self.signature(names, date, payload_hash), sent)

    def reply(self):
        path = self.path.partition("?")[0]
        if "chunked" in self.headers.get("Transfer-Encoding", "").lower():
            body = b""  # `RestCatalog::sendRequest` sends the JSON body of a POST in chunks.
            while (size := int(self.rfile.readline().split(b";")[0], 16)):
                body += self.rfile.read(size)
                self.rfile.read(2)
            self.rfile.readline()
        else:
            body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        need, status, out = "x-amz-date", 200, b'{"namespaces":[],"identifiers":[]}'
        if path.endswith("/config"):
            out = b'{"defaults":{},"overrides":{}}'
        elif path.endswith("/tables/tbl"):
            need, out = "x-iceberg-access-delegation", b""
        elif self.command == "POST":
            # Only the namespace creation posts; 409 means "another client created it", i.e. accepted.
            need, status, out = "content-type", 409, b"{}"
        elif path.endswith("/namespaces/nspace"):
            status, out = 404, b"{}"
        if not self.accepted(need, body):
            status, out = 401, b"{}"
        self.send_response(status)
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)

    do_GET = do_POST = do_DELETE = reply

    def log_message(self, *args):
        pass  # Keep stderr empty; a traceback is still reported.

server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
open(FIFO, "w").write(str(server.server_address[1]) + "\n")
server.serve_forever()
EOF
STUB_PID=$!
trap '${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB}"; kill ${STUB_PID} 2>/dev/null; rm -f "${FIFO}"' EXIT
read -r PORT < "$FIFO"

err_code() {
    ${CLICKHOUSE_CLIENT} --query "$1" 2>&1 | grep -oE '\([A-Z][A-Z_0-9]+\)' | tail -1
}

# CREATE DATABASE builds the catalog eagerly, signing `GET /v1/config?warehouse=wh` with no payload.
${CLICKHOUSE_CLIENT} --query "
    SET allow_database_iceberg = 1;
    CREATE DATABASE ${DB} ENGINE = DataLakeCatalog('http://127.0.0.1:${PORT}/v1')
    SETTINGS catalog_type = 's3tables', region = 'us-east-1', warehouse = 'wh',
        aws_access_key_id = 'key_${CLICKHOUSE_DATABASE}', aws_secret_access_key = 'sec_${CLICKHOUSE_DATABASE}';
    SELECT count() FROM system.databases WHERE name = '${DB}'"

# A table lookup adds `X-Iceberg-Access-Delegation`; the empty body means "no such table".
err_code "SELECT count() FROM ${DB}.\`nspace.tbl\`"

# A table without a location creates the namespace: a GET answered 404, then a POST with a body.
err_code "CREATE TABLE ${DB}.\`nspace.tbl\` (x UInt8)"
