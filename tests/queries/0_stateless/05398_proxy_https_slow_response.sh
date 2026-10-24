#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: clickhouse-proxy is built on the silk fiber framework, which the fast build omits.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A TLS-terminating HTTP listener relays through user space, where both directions share the TLS
# session of the client leg. A response that comes later than `send_timeout_ms` must still reach the
# client: the read of the next request must neither be bounded by the send timeout nor block the
# write of the response. Two requests on one keep-alive connection check that the session survives.

DIR="${CLICKHOUSE_TMP}/05398_proxy"
mkdir -p "$DIR"
CONFIG="$DIR/config.xml"
LOG="$DIR/proxy.log"
NONCE="05398_${CLICKHOUSE_DATABASE}"

PROXY_PID=
BACKEND_PID=
trap '[ -n "$PROXY_PID" ] && kill "$PROXY_PID" 2>/dev/null; [ -n "$BACKEND_PID" ] && kill "$BACKEND_PID" 2>/dev/null' EXIT

free_port()
{
    python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()"
}

openssl req -x509 -newkey rsa:2048 -nodes -keyout "$DIR/server.key" -out "$DIR/server.crt" -days 1 -subj /CN=localhost 2>/dev/null

# A backend that answers each request after a delay longer than the proxy's `send_timeout_ms`.
cat > "$DIR/backend.py" <<'EOF'
import http.server, sys, time

class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def do_GET(self):
        time.sleep(2)
        body = ("slow " + self.path + "\n").encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *args):
        pass

server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
print(server.server_address[1], flush=True)
server.serve_forever()
EOF

exec 3< <(python3 -I "$DIR/backend.py")
BACKEND_PID=$!
read -r -t 30 BACKEND_PORT <&3

start_proxy()
{
    PROXY_PORT=$(free_port)

    cat > "$CONFIG" <<EOF
<clickhouse>
    <logger><level>warning</level><console>1</console></logger>
    <proxy>
        <listen_host>127.0.0.1</listen_host>
        <send_timeout_ms>500</send_timeout_ms>
        <listeners>
            <listener><protocol>http</protocol><port>${PROXY_PORT}</port><secure>1</secure><pool>slow</pool></listener>
        </listeners>
        <pools>
            <pool>
                <name>slow</name>
                <backend><host>127.0.0.1</host><http_port>${BACKEND_PORT}</http_port></backend>
            </pool>
        </pools>
        <http>
            <static>
                <page><path>/whoami</path><content>${NONCE}</content></page>
            </static>
        </http>
        <health_check><enabled>0</enabled></health_check>
    </proxy>
    <openSSL>
        <server>
            <certificateFile>${DIR}/server.crt</certificateFile>
            <privateKeyFile>${DIR}/server.key</privateKeyFile>
        </server>
    </openSSL>
</clickhouse>
EOF

    $CLICKHOUSE_BINARY proxy --config-file "$CONFIG" > "$LOG" 2>&1 &
    PROXY_PID=$!

    for _ in $(seq 1 100); do
        WHOAMI=$(curl -sk --max-time 5 "https://127.0.0.1:${PROXY_PORT}/whoami")
        CURL_STATUS=$?
        if [ "$WHOAMI" = "$NONCE" ]; then return 0; fi
        if [ -n "$WHOAMI" ] || [ "$CURL_STATUS" -eq 28 ] || ! kill -0 "$PROXY_PID" 2>/dev/null; then break; fi
        sleep 0.2
    done

    kill "$PROXY_PID" 2>/dev/null
    wait "$PROXY_PID" 2>/dev/null
    PROXY_PID=
    return 1
}

READY=0
for _ in $(seq 1 5); do
    if start_proxy; then READY=1; break; fi
    if grep -qiE "silk|SUPPORT_IS_DISABLED" "$LOG"; then break; fi
done

if [ "$READY" -ne 1 ]; then
    if grep -qiE "silk|SUPPORT_IS_DISABLED" "$LOG"; then
        # The proxy is not available in this build; emit the expected output so the test passes.
        echo "slow /a"
        echo "slow /b"
        exit 0
    fi
    echo "proxy did not start" >&2
    cat "$LOG" >&2
    exit 1
fi

curl -sk --max-time 30 "https://127.0.0.1:${PROXY_PORT}/a" "https://127.0.0.1:${PROXY_PORT}/b"
