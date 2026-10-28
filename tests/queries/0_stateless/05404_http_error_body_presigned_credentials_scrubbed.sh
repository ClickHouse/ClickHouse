#!/usr/bin/env bash

# A presigned URL carries its credentials in the query string, and a server rejecting it can echo them outside the
# URL: an S3 `SignatureDoesNotMatch` body has `<AWSAccessKeyId>` and `<SignatureProvided>`. That body is copied into
# the exception, so the credentials must be scrubbed there as well, while the rest of the body is kept.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The server binds a free port itself and reports it in a file, so that no other test can take the port meanwhile.
PORT_FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.port"
rm -f "$PORT_FILE"

# Answers like S3 answers a presigned URL whose signature does not match: the body echoes the access key id, the
# signature as decoded from the query, and the canonical request, i.e. the query without the signature, with '&'
# escaped as XML requires. Kept on one line so the client prints it on the line of the error.
python3 - "$PORT_FILE" <<'EOF' &
import os
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
        params = dict(urllib.parse.parse_qsl(query))
        sent = None
        if path == "/ready":
            body = b"ready"
            self.send_response(200)
        elif path.startswith("/ranges/"):
            # Answers a range GET with the status in the path, and the signature and the bearer token as its reason
            # phrase. A plain GET is cut short, so that the read is resumed with a range. The close is announced: the
            # client could reuse a silently closed connection before it notices the close.
            body = b"1\n" * 32
            if "Range" in self.headers:
                token = self.headers["Authorization"].removeprefix("Bearer ")
                self.send_response(int(path.split("/")[2]), f'{params["X-Amz-Signature"]} {token}')
            else:
                self.send_response(200)
                sent = body[:8]
                self.send_header("Connection", "close")
            self.send_header("Accept-Ranges", "bytes")
        else:
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
            self.wfile.write(body if sent is None else sent)

    def do_GET(self):
        self.reply(True)

    def do_POST(self):
        self.reply(True)

    def do_HEAD(self):
        self.reply(False)

    def log_message(self, *args):
        pass


class Server(ThreadingHTTPServer):
    # A failed parallel read resets the connections of its other readers.
    def handle_error(self, request, client_address):
        if not isinstance(sys.exc_info()[1], ConnectionError):
            super().handle_error(request, client_address)


server = Server(("127.0.0.1", 0), Handler)
with open(sys.argv[1] + ".tmp", "w") as f:
    f.write(str(server.server_address[1]))
os.replace(sys.argv[1] + ".tmp", sys.argv[1])
server.serve_forever()
EOF
HTTP_PID=$!
trap 'kill $HTTP_PID 2>/dev/null; rm -f "$PORT_FILE"' EXIT

for _ in {1..100}; do
    [ -s "$PORT_FILE" ] && break
    sleep 0.1
done
HTTP_PORT=$(cat "$PORT_FILE")
for _ in {1..100}; do
    [ "$(${CLICKHOUSE_CURL} -s "http://127.0.0.1:${HTTP_PORT}/ready")" = "ready" ] && break
    sleep 0.1
done

U="${CLICKHOUSE_TEST_UNIQUE_NAME}"
KEY_V4="AKIAV4${U}"
KEY_V4_SENT="AKIA%564${U}"  # the same key id as sent, its 'V' percent-encoded
SIG_V4="sigv4${U}"
TOKEN="tokv4${U}"
KEY_V2="AKIAV2${U}"
SIG_V2="sigv2${U}"
TOKEN_V2="tokv2${U}"
TOKEN_EXP="tokexp${U}"
SIG_ENC="sigenc${U}"
OVL="ovl${U}"
SIG_RNG="sigrng${U}"
SIG_SAS="sigsas${U}"
TOK_RNG="tokrng${U}"
BASE="http://127.0.0.1:${HTTP_PORT}/bucket/key.csv"
URL_V4="${BASE}?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=${KEY_V4_SENT}%2F20261016%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20261016T000000Z&X-Amz-Expires=900&X-Amz-Security-Token=${TOKEN}%2Fp%2B2%3D&X-Amz-SignedHeaders=host&X-Amz-Signature=${SIG_V4}"
URL_V2="${BASE}?AWSAccessKeyId=${KEY_V2}&Expires=1792200000&Signature=${SIG_V2}%2Bp%2F2%3D"
# The S3 Express session token, the SigV2 one as presigners spell it, and a signature whose name is percent-encoded: the
# peer matches names decoded. The URI itself is masked by name as typed, so only the body is checked for this one.
URL_ALT="${BASE}?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-S3session-Token=${TOKEN_EXP}&x-amz-security-token=${TOKEN_V2}&X-Amz-%53ignature=${SIG_ENC}"
# A short credential repeated: each '[HIDDEN]' has two 'D', so a scrub that searched its own output would grow exponentially.
URL_DUP="${BASE}?$(printf 'Signature=D&%.0s' {1..18})Expires=1792200000"
# The signature is a prefix of the token and comes first: scrubbing it first would leave the token's tail visible.
URL_OVL="${BASE}?X-Amz-Signature=${OVL}&X-Amz-Security-Token=${OVL}tail&X-Amz-Date=20261016T000000Z"
# An Azure SAS: `sig` is its signature, the other fields stay visible.
URL_SAS="${BASE}?sv=2022-11-02&sp=r&sig=${SIG_SAS}"
URL_RNG="http://127.0.0.1:${HTTP_PORT}/ranges/200/key.csv?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=${SIG_RNG}"
URL_RNG_201="http://127.0.0.1:${HTTP_PORT}/ranges/201/key.csv?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=${SIG_RNG}"
HDR_RNG="headers('Authorization' = 'Bearer ${TOK_RNG}')"

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

V4=(-"$KEY_V4" -"$KEY_V4_SENT" -"$SIG_V4" -"$TOKEN"
    +"<AWSAccessKeyId>[HIDDEN]</AWSAccessKeyId>" +"<SignatureProvided>[HIDDEN]</SignatureProvided>"
    +"&amp;X-Amz-Security-Token=[HIDDEN]&amp;"
    +"<Code>SignatureDoesNotMatch</Code>" +"&amp;X-Amz-Date=20261016T000000Z&amp;" +"<RequestId>probe-request-id</RequestId>")
V2=(-"$KEY_V2" -"$SIG_V2"
    +"<AWSAccessKeyId>[HIDDEN]</AWSAccessKeyId>" +"<SignatureProvided>[HIDDEN]</SignatureProvided>"
    +"<Code>SignatureDoesNotMatch</Code>" +"&amp;Expires=1792200000" +"<RequestId>probe-request-id</RequestId>")
ALT=(-"$TOKEN_EXP" -"$TOKEN_V2" -"$SIG_ENC" +"<SignatureProvided>[HIDDEN]</SignatureProvided>"
    +"X-Amz-S3session-Token=[HIDDEN]&amp;x-amz-security-token=[HIDDEN]&amp;X-Amz-%53ignature=[HIDDEN]</CanonicalRequest>")
OV=(-"$OVL" +"<SignatureProvided>[HIDDEN]</SignatureProvided>"
    +"X-Amz-Security-Token=[HIDDEN]&amp;X-Amz-Date=20261016T000000Z")
RNG=(-"$SIG_RNG" -"$TOK_RNG" +"HTTP status code: 416 '[HIDDEN] [HIDDEN]'" +"reason: [HIDDEN] [HIDDEN]), will retry")

# Grep the error line: the client also echoes the submitted query, which carries the URL as typed.
error_line() {
    ${CLICKHOUSE_CLIENT} --query "$1" 2>&1 | grep -F "${2:-Received error from remote server}"
}

body_of() {
    local body=${1#*body: }
    echo "${body%%</Error>*}"
}

check select_sigv4 "$(error_line "SELECT * FROM url('${URL_V4}', 'CSV', 'x UInt8')")" "${V4[@]}"
check insert_sigv4 "$(error_line "INSERT INTO TABLE FUNCTION url('${URL_V4}', 'CSV', 'x UInt8') VALUES (1)")" "${V4[@]}"
check select_sigv2 "$(error_line "SELECT * FROM url('${URL_V2}', 'CSV', 'x UInt8')")" "${V2[@]}"
check select_overlap "$(error_line "SELECT * FROM url('${URL_OVL}', 'CSV', 'x UInt8')")" "${OV[@]}"
check select_name_spellings "$(body_of "$(error_line "SELECT * FROM url('${URL_ALT}', 'CSV', 'x UInt8')")")" "${ALT[@]}"
check select_azure_sas "$(error_line "SELECT * FROM url('${URL_SAS}', 'CSV', 'x UInt8')")" \
    -"$SIG_SAS" +"sv=2022-11-02&amp;sp=r&amp;sig=[HIDDEN]</CanonicalRequest>"
# A range that the server does not serve, in a parallel read and in a sequential read resumed after the connection is cut.
check select_parallel_range "$(error_line "SELECT * FROM url('${URL_RNG}', 'CSV', 'x UInt8', ${HDR_RNG})
    SETTINGS max_download_threads = 2, max_download_buffer_size = 1, http_max_tries = 1, http_make_head_request = 1,
        parallel_replicas_for_cluster_engines = 0")" "${RNG[@]}" +"readBigAt"
check select_resumed_range "$(error_line "SELECT * FROM url('${URL_RNG}', 'CSV', 'x UInt8', ${HDR_RNG})
    SETTINGS max_download_threads = 1, max_read_buffer_size = 4, http_max_tries = 2")" "${RNG[@]}" +"range: [8, -]"
# A status other than 200 is not retried and fails with a plain exception.
check select_resumed_range_201 "$(error_line "SELECT * FROM url('${URL_RNG_201}', 'CSV', 'x UInt8', ${HDR_RNG})
    SETTINGS max_download_threads = 1, max_read_buffer_size = 4, http_max_tries = 2" 'Cannot read with range')" \
    -"$SIG_RNG" -"$TOK_RNG" -"will retry" +"range: [8, -]" +"reason: [HIDDEN] [HIDDEN])"
# The server log can be sent along with the error, so the bound is per line.
LINES=$(error_line "SELECT * FROM url('${URL_DUP}', 'CSV', 'x UInt8')")
LONGEST=$(awk '{ if (length($0) > n) n = length($0) } END { print n + 0 }' <<< "$LINES")
if [ "$LONGEST" -lt 4096 ]; then
    check select_duplicates "$LINES" +"<SignatureProvided>[HIDDEN]</SignatureProvided>"
else
    echo "select_duplicates: FAIL a line of $LONGEST bytes"
fi

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
# The errors are logged, scrubbed.
${CLICKHOUSE_CLIENT} --query "
    SELECT count() > 0 FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'ExceptionWhileProcessing'
      AND exception LIKE '%<SignatureProvided>[HIDDEN]</SignatureProvided>%'"
# Nothing is selected on success. The needles are split so that this query does not itself contain a credential.
${CLICKHOUSE_CLIENT} --query "
    SELECT type, exception FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase()
      AND (exception LIKE '%' || 'sigv4' || '${U}%' OR exception LIKE '%' || 'sigv2' || '${U}%'
        OR exception LIKE '%' || 'AKIAV4' || '${U}%' OR exception LIKE '%' || 'AKIAV2' || '${U}%'
        OR exception LIKE '%' || 'tokv4' || '${U}%' OR exception LIKE '%' || 'ovl' || '${U}%'
        OR exception LIKE '%' || 'tokexp' || '${U}%' OR exception LIKE '%' || 'sigrng' || '${U}%'
        OR exception LIKE '%' || 'tokrng' || '${U}%'
        OR position(exception, 'AKIA%564' || '${U}') > 0)
    FORMAT Vertical"
