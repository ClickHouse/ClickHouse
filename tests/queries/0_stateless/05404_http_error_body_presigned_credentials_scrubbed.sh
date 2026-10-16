#!/usr/bin/env bash

# A presigned URL carries its credentials in the query string, and a server rejecting it can echo them outside the
# URL: an S3 `SignatureDoesNotMatch` body has `<AWSAccessKeyId>` and `<SignatureProvided>`. That body is copied into
# the exception, so the credentials must be scrubbed there as well, while the rest of the body is kept.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

HTTP_PORT=$(python3 -c "
import socket
s = socket.socket()
s.bind(('127.0.0.1', 0))
print(s.getsockname()[1])
s.close()
")

# Answers like S3 answers a presigned URL whose signature does not match: the body echoes the access key id, the
# signature as decoded from the query, and the canonical request, i.e. the query without the signature, with '&'
# escaped as XML requires. Kept on one line so the client prints it on the line of the error.
python3 - "$HTTP_PORT" <<'EOF' &
import sys
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def drain(self):
        if "chunked" in (self.headers.get("Transfer-Encoding") or "").lower():
            while True:
                size = int(self.rfile.readline().split(b";")[0].strip() or b"0", 16)
                if size == 0:
                    self.rfile.readline()
                    return
                self.rfile.read(size + 2)
        else:
            self.rfile.read(int(self.headers.get("Content-Length") or 0))

    def reply(self, with_body):
        self.drain()
        path, _, query = self.path.partition("?")
        if path == "/ready":
            body = b"ready"
            self.send_response(200)
        else:
            params = dict(urllib.parse.parse_qsl(query))
            key_id = params.get("X-Amz-Credential", "").split("/")[0] or params.get("AWSAccessKeyId", "")
            signature = params.get("X-Amz-Signature") or params.get("Signature", "")
            canonical_query = "&amp;".join(
                p for p in query.split("&") if not p.startswith(("X-Amz-Signature=", "Signature=")))
            body = (
                '<?xml version="1.0" encoding="UTF-8"?><Error><Code>SignatureDoesNotMatch</Code>'
                f"<AWSAccessKeyId>{key_id}</AWSAccessKeyId><SignatureProvided>{signature}</SignatureProvided>"
                f"<CanonicalRequest>{self.command} {path} {canonical_query}</CanonicalRequest>"
                "<RequestId>probe-request-id</RequestId></Error>"
            ).encode()
            self.send_response(403)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if with_body:
            self.wfile.write(body)

    def do_GET(self):
        self.reply(True)

    def do_POST(self):
        self.reply(True)

    def do_HEAD(self):
        self.reply(False)

    def log_message(self, *args):
        pass


ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), Handler).serve_forever()
EOF
HTTP_PID=$!
trap 'kill $HTTP_PID 2>/dev/null' EXIT

for _ in {1..100}; do
    [ "$(${CLICKHOUSE_CURL} -s "http://127.0.0.1:${HTTP_PORT}/ready")" = "ready" ] && break
    sleep 0.1
done

U="${CLICKHOUSE_TEST_UNIQUE_NAME}"
KEY_V4="AKIAV4${U}"
SIG_V4="sigv4${U}"
TOKEN="tokv4${U}"
KEY_V2="AKIAV2${U}"
SIG_V2="sigv2${U}"
OVL="ovl${U}"
BASE="http://127.0.0.1:${HTTP_PORT}/bucket/key.csv"
URL_V4="${BASE}?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=${KEY_V4}%2F20261016%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20261016T000000Z&X-Amz-Expires=900&X-Amz-Security-Token=${TOKEN}%2Fp%2B2%3D&X-Amz-SignedHeaders=host&X-Amz-Signature=${SIG_V4}"
URL_V2="${BASE}?AWSAccessKeyId=${KEY_V2}&Expires=1792200000&Signature=${SIG_V2}%2Bp%2F2%3D"
# The signature is a prefix of the token and comes first: scrubbing it first would leave the token's tail visible.
URL_OVL="${BASE}?X-Amz-Signature=${OVL}&X-Amz-Security-Token=${OVL}tail&X-Amz-Date=20261016T000000Z"

# OK only if no credential survives (-), and every expected text is present (+): the scrubbed shapes prove the body
# was echoed and scrubbed rather than absent, and the non-secret parts prove it was not blanked wholesale.
check() {
    local label=$1 line=$2 arg
    shift 2
    for arg in "$@"; do
        case "$arg" in
            -*) if grep -qF -- "${arg#-}" <<< "$line"; then echo "$label: FAIL leaked ${arg#-}"; return; fi ;;
            +*) if ! grep -qF -- "${arg#+}" <<< "$line"; then echo "$label: FAIL missing ${arg#+}"; return; fi ;;
        esac
    done
    echo "$label: OK"
}

V4=(-"$KEY_V4" -"$SIG_V4" -"$TOKEN"
    +"<AWSAccessKeyId>[HIDDEN]</AWSAccessKeyId>" +"<SignatureProvided>[HIDDEN]</SignatureProvided>"
    +"&amp;X-Amz-Security-Token=[HIDDEN]&amp;"
    +"<Code>SignatureDoesNotMatch</Code>" +"&amp;X-Amz-Date=20261016T000000Z&amp;" +"<RequestId>probe-request-id</RequestId>")
V2=(-"$KEY_V2" -"$SIG_V2"
    +"<AWSAccessKeyId>[HIDDEN]</AWSAccessKeyId>" +"<SignatureProvided>[HIDDEN]</SignatureProvided>"
    +"<Code>SignatureDoesNotMatch</Code>" +"&amp;Expires=1792200000" +"<RequestId>probe-request-id</RequestId>")
OV=(-"$OVL" +"<SignatureProvided>[HIDDEN]</SignatureProvided>"
    +"X-Amz-Security-Token=[HIDDEN]&amp;X-Amz-Date=20261016T000000Z")

# Grep the error line: the client also echoes the submitted query, which carries the URL as typed.
error_line() {
    ${CLICKHOUSE_CLIENT} --query "$1" 2>&1 | grep -F 'Received error from remote server'
}

check select_sigv4 "$(error_line "SELECT * FROM url('${URL_V4}', 'CSV', 'x UInt8')")" "${V4[@]}"
check insert_sigv4 "$(error_line "INSERT INTO TABLE FUNCTION url('${URL_V4}', 'CSV', 'x UInt8') VALUES (1)")" "${V4[@]}"
check select_sigv2 "$(error_line "SELECT * FROM url('${URL_V2}', 'CSV', 'x UInt8')")" "${V2[@]}"
check select_overlap "$(error_line "SELECT * FROM url('${URL_OVL}', 'CSV', 'x UInt8')")" "${OV[@]}"

# Nothing is selected on success. The needles are split so that this query does not itself contain a credential.
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT type, exception FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase()
      AND (exception LIKE '%' || 'sigv4' || '${U}%' OR exception LIKE '%' || 'sigv2' || '${U}%'
        OR exception LIKE '%' || 'AKIAV4' || '${U}%' OR exception LIKE '%' || 'AKIAV2' || '${U}%'
        OR exception LIKE '%' || 'tokv4' || '${U}%' OR exception LIKE '%' || 'ovl' || '${U}%')
    FORMAT Vertical"
