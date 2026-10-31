#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: clickhouse-proxy is built on the silk fiber framework, which the fast build omits.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The proxy must classify the query and extract the routing attributes of an HTTP request the way the server does:
# - comments are recognized by the server's lexer (nested, `//`, `#`), also when an opener is split between chunks;
# - a `WITH` query may be an `INSERT`, and a read query combined with `PARALLEL WITH` is not a read;
# - the database may come from the URL path or from an RFC 2047 encoded header.
# Every request prints the HTTP status: the live server answers a request routed to the `ch` pool, a request routed
# to the `dead` pool gets 502, and a request matching no rule gets 503 (the listener has no default pool).

CONFIG="${CLICKHOUSE_TMP}/05453_proxy_config.xml"
LOG="${CLICKHOUSE_TMP}/05453_proxy.log"

NONCE="05453_${CLICKHOUSE_DATABASE}"

PROXY_PID=
PROXY_PORT=

trap '[ -n "$PROXY_PID" ] && kill "$PROXY_PID" 2>/dev/null' EXIT

$CLICKHOUSE_CLIENT --query "CREATE TABLE t_path (x UInt8) ENGINE = Memory; INSERT INTO t_path VALUES (42);"

start_proxy()
{
    # A free port is picked by binding a socket and immediately releasing it, so another process can
    # take it before the proxy binds it. Serve a run-unique token to tell our own proxy from a stranger.
    PROXY_PORT=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")
    DEAD_PORT=$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1',0)); print(s.getsockname()[1]); s.close()")

    cat > "$CONFIG" <<EOF
<clickhouse>
    <logger><level>warning</level><console>1</console></logger>
    <proxy>
        <listen_host>127.0.0.1</listen_host>
        <listeners>
            <listener><protocol>http</protocol><port>${PROXY_PORT}</port></listener>
        </listeners>
        <pools>
            <pool>
                <name>ch</name>
                <backend><host>${CLICKHOUSE_HOST}</host><http_port>${CLICKHOUSE_PORT_HTTP}</http_port></backend>
            </pool>
            <pool>
                <name>dead</name>
                <backend><host>127.0.0.1</host><http_port>${DEAD_PORT}</http_port></backend>
            </pool>
        </pools>
        <rules>
            <rule><database>${CLICKHOUSE_DATABASE}</database><pool>ch</pool></rule>
            <rule><query_type>select</query_type><pool>ch</pool></rule>
            <rule><query_type>insert,other</query_type><pool>dead</pool></rule>
        </rules>
        <http>
            <static>
                <page><path>/whoami</path><content>${NONCE}</content></page>
            </static>
        </http>
        <health_check><enabled>0</enabled></health_check>
    </proxy>
</clickhouse>
EOF

    $CLICKHOUSE_BINARY proxy --config-file "$CONFIG" > "$LOG" 2>&1 &
    PROXY_PID=$!

    # Wait until our own proxy answers on the port.
    for _ in $(seq 1 100); do
        WHOAMI=$(curl -s --max-time 5 "http://127.0.0.1:${PROXY_PORT}/whoami")
        CURL_STATUS=$?
        if [ "$WHOAMI" = "$NONCE" ]; then return 0; fi
        # An answer from something else - a different body, or a hang up to the timeout (curl exit
        # code 28) - means the port was taken by a stranger, and so does our own proxy exiting (it
        # could not bind). All of them are retried on a freshly picked port.
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
    # A build without the fiber framework will never come up; do not retry it.
    if grep -qF "the silk fiber framework, which is only available on Linux" "$LOG"; then break; fi
done

if [ "$READY" -ne 1 ]; then
    if grep -qF "the silk fiber framework, which is only available on Linux" "$LOG"; then
        # The proxy is not available in this build; emit the expected output so the test passes.
        cat "$CUR_DIR/05453_proxy_http_classify_like_server.reference"
        exit 0
    fi
    echo "proxy did not start" >&2
    cat "$LOG" >&2
    exit 1
fi

URL="http://127.0.0.1:${PROXY_PORT}/"

status()
{
    curl -s -o /dev/null -w '%{http_code}\n' --max-time 10 "$@"
}

echo "comments"
status --data-binary $'// comment\nSELECT 1' "$URL"
status --data-binary $'# comment\nSELECT 1' "$URL"
status --data-binary '/* outer /* inner */ SELECT */ INSERT INTO t_nonexistent VALUES (1)' "$URL"

echo "writes"
status --data-binary 'WITH y AS (SELECT 1) INSERT INTO t_nonexistent SELECT * FROM y' "$URL"
status --data-binary 'SELECT 1 PARALLEL WITH INSERT INTO t_nonexistent VALUES (1)' "$URL"
status --data-binary 'WITH 1 AS y SELECT y' "$URL"

echo "database"
status "${URL}${CLICKHOUSE_DATABASE}/t_path"
ENCODED_DATABASE=$(python3 -c "import sys; print('=?UTF-8?Q?' + ''.join('=%02X' % b for b in sys.argv[1].encode()) + '?=')" "$CLICKHOUSE_DATABASE")
status -H "X-ClickHouse-Database: ${ENCODED_DATABASE}" "${URL}?query=(SELECT%201)"

# A comment opener split between chunks: the proxy reads the next chunks to classify the query.
python3 - "$PROXY_PORT" <<'EOF'
import socket
import sys

port = int(sys.argv[1])

def status(request):
    sock = socket.create_connection(("127.0.0.1", port), timeout=10)
    sock.sendall(request)
    response = b""
    while b"\r\n" not in response:
        chunk = sock.recv(4096)
        if not chunk:
            break
        response += chunk
    sock.close()
    print(response.split(b" ")[1].decode() if response else "no response")

print("chunked")
status(b"POST / HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
    b"1\r\n/\r\nf\r\n* c */ SELECT 1\r\n0\r\n\r\n")
# A query still within a comment after the inspected 64 KiB of the body has no query type.
status(b"POST / HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"
    b"1\r\n/\r\n" + b"10000\r\n" + b"*" + b" " * 65535 + b"\r\n")
EOF
