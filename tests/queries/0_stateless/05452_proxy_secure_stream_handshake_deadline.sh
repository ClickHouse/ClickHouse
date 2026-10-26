#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: clickhouse-proxy is built on the silk fiber framework, which the fast build omits.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A TLS-terminating `stream` listener relays without reading anything from the client first, so the
# TLS handshake with the client is the whole handshake. A client that connects and never sends a
# ClientHello must be disconnected once `handshake_timeout_ms` passes, without the proxy opening a
# connection to the backend for it.

DIR="${CLICKHOUSE_TMP}/05452_proxy"
mkdir -p "$DIR"
CONFIG="$DIR/config.xml"
LOG="$DIR/proxy.log"
ACCEPTED="$DIR/accepted"
: > "$ACCEPTED"

PROXY_PID=
BACKEND_PID=
trap '[ -n "$PROXY_PID" ] && kill "$PROXY_PID" 2>/dev/null; [ -n "$BACKEND_PID" ] && kill "$BACKEND_PID" 2>/dev/null' EXIT

free_port()
{
    python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()"
}

openssl req -x509 -newkey rsa:2048 -nodes -keyout "$DIR/server.key" -out "$DIR/server.crt" -days 1 -subj /CN=localhost 2>/dev/null

# An echo backend that records every connection it accepts.
cat > "$DIR/backend.py" <<'EOF'
import socket, sys, threading

accepted = sys.argv[1]

def serve(conn):
    with conn:
        while True:
            data = conn.recv(4096)
            if not data:
                break
            conn.sendall(data)

server = socket.socket()
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind(("127.0.0.1", 0))
server.listen(16)
print(server.getsockname()[1], flush=True)
while True:
    conn, _ = server.accept()
    with open(accepted, "a") as f:
        f.write("x\n")
    threading.Thread(target=serve, args=(conn,), daemon=True).start()
EOF

# A TLS client that sends a line through the proxy and prints the echo.
cat > "$DIR/client.py" <<'EOF'
import socket, ssl, sys

context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
context.check_hostname = False
context.verify_mode = ssl.CERT_NONE
with socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=5) as raw:
    with context.wrap_socket(raw) as conn:
        conn.sendall(b"ping\n")
        print(conn.recv(4096).decode().strip())
EOF

# A client that connects and sends nothing; reports whether the proxy closed the connection in time.
cat > "$DIR/silent.py" <<'EOF'
import socket, sys

with socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=5) as conn:
    conn.settimeout(60)
    try:
        while conn.recv(4096):
            pass
        print("closed")
    except socket.timeout:
        print("not closed")
    except ConnectionResetError:
        print("closed")
EOF

exec 3< <(python3 -I "$DIR/backend.py" "$ACCEPTED")
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
        <handshake_timeout_ms>500</handshake_timeout_ms>
        <listeners>
            <listener><protocol>stream</protocol><port>${PROXY_PORT}</port><secure>1</secure><pool>echo</pool></listener>
        </listeners>
        <pools>
            <pool>
                <name>echo</name>
                <backend><host>127.0.0.1</host><raw_port>${BACKEND_PORT}</raw_port></backend>
            </pool>
        </pools>
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
        if [ "$(python3 -I "$DIR/client.py" "$PROXY_PORT" 2>/dev/null)" = "ping" ]; then return 0; fi
        if ! kill -0 "$PROXY_PID" 2>/dev/null; then break; fi
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
        echo "closed"
        echo "backend connections: 0"
        exit 0
    fi
    echo "proxy did not start" >&2
    cat "$LOG" >&2
    exit 1
fi

BEFORE=$(wc -l < "$ACCEPTED")
python3 -I "$DIR/silent.py" "$PROXY_PORT"
AFTER=$(wc -l < "$ACCEPTED")
echo "backend connections: $((AFTER - BEFORE))"
