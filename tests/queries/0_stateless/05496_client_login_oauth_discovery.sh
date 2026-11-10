#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the client in the fast test build is compiled without OAuth support, so --login does nothing there.

# `clickhouse client --login --oauth-url <issuer>` discovers the device flow endpoints instead of taking them
# from --oauth-device-uri / --oauth-token-uri (docs/en/interfaces/cli.md, "OAuth 2.0 device authorization grant").
# It tries the well-known documents of the issuer in turn, skips a document that does not advertise the device
# code grant or that carries no endpoints, and reports BAD_ARGUMENTS when no candidate yields a pair of endpoints.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$DIR/bin"
printf '#!/bin/sh\nexit 0\n' > "$DIR/bin/xdg-open"
chmod +x "$DIR/bin/xdg-open"
mkfifo "$DIR/port"

openssl req -newkey rsa:2048 -nodes -keyout "$DIR/key.pem" -x509 -days 1 -out "$DIR/cert.pem" \
    -subj "/CN=localhost" > /dev/null 2>&1

cat > "$DIR/idp.py" <<'EOF'
import http.server, json, ssl, sys

DEVICE_GRANT = "urn:ietf:params:oauth:grant-type:device_code"

class Handler(http.server.BaseHTTPRequestHandler):
    def reply(self, status, body):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        # The candidates of an issuer are <issuer>/.well-known/openid-configuration,
        # <issuer>/.well-known/oauth-authorization-server and then the two RFC 8414
        # insertion style URLs, which keep the issuer path last and get a 404 here.
        if self.path == "/cov/.well-known/openid-configuration":
            self.reply(200, {"grant_types_supported": ["authorization_code"]})
        elif self.path == "/cov/.well-known/oauth-authorization-server":
            self.reply(200, {"grant_types_supported": [DEVICE_GRANT], "token_endpoint": BASE + "/token"})
        elif self.path == "/ok/.well-known/openid-configuration":
            self.reply(200, {"grant_types_supported": [DEVICE_GRANT], "token_endpoint": BASE + "/token",
                             "device_authorization_endpoint": BASE + "/device"})
        else:
            self.reply(404, {"error": "not_found"})

    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        # `expires_in: 0` ends the device code flow right after the user code has been printed,
        # so the client never polls the token endpoint and the test never waits for an interval.
        self.reply(200, {"device_code": "dc", "user_code": "ZZZ-COV", "expires_in": 0,
                         "verification_uri": "https://localhost/verify"})

    def log_message(self, *args):
        pass

server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
BASE = "https://127.0.0.1:%d" % server.server_address[1]
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain(sys.argv[1] + "/cert.pem", sys.argv[1] + "/key.pem")
server.socket = context.wrap_socket(server.socket, server_side=True)
print(server.server_address[1], flush=True)
server.serve_forever()
EOF

python3 "$DIR/idp.py" "$DIR" > "$DIR/port" 2>/dev/null &
IDP_PID=$!
trap '{ kill $IDP_PID; wait $IDP_PID; } 2>/dev/null; rm -rf "$DIR"' EXIT
read -r PORT < "$DIR/port"

login() {
    PATH="$DIR/bin:$PATH" $CLICKHOUSE_CLIENT_BINARY --host "$CLICKHOUSE_HOST" --port "$CLICKHOUSE_PORT_TCP" \
        --login --oauth-client-id test --oauth-url "$1" --accept-invalid-certificate -q "SELECT 1" 2>&1
}

# No candidate document of the issuer yields endpoints: the first one does not advertise the device code
# grant, the second one has no device_authorization_endpoint, the two insertion style URLs answer 404.
login "https://127.0.0.1:$PORT/cov" | grep -q -F "BAD_ARGUMENTS" && echo "no endpoints BAD_ARGUMENTS"
# Nothing listens on the issuer, so every candidate URL fails before a document is read.
login "https://127.0.0.1:1" | grep -q -F "BAD_ARGUMENTS" && echo "unreachable BAD_ARGUMENTS"
# The first candidate document carries both endpoints: the client asks the discovered
# device_authorization_endpoint for a device code and prints the user code it answered with.
login "https://127.0.0.1:$PORT/ok" | grep -q -F "ZZZ-COV" && echo "discovered ZZZ-COV"
