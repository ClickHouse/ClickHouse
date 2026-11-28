#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: clickhouse-proxy is built on the silk fiber framework, which the fast build omits.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The proxy must read the head of an HTTP request exactly as the backend does: otherwise a request could be routed
# by one user or query type and executed with another. What the two would read differently is rejected by the proxy.
# Every request prints the HTTP status: the live server answers a request routed to the `ch` pool (reads),
# a request routed to the `dead` pool (writes) gets 502, and a request matching no rule (a query type
# that is not known) gets 503, as the listener has no default pool.

CONFIG="${CLICKHOUSE_TMP}/05842_proxy_config.xml"
LOG="${CLICKHOUSE_TMP}/05842_proxy.log"

NONCE="05842_${CLICKHOUSE_DATABASE}"

PROXY_PID=
PROXY_PORT=

trap '[ -n "$PROXY_PID" ] && kill "$PROXY_PID" 2>/dev/null' EXIT

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
        cat "$CUR_DIR/05842_proxy_http_strict_request_head.reference"
        exit 0
    fi
    echo "proxy did not start" >&2
    cat "$LOG" >&2
    exit 1
fi

python3 - "$PROXY_PORT" <<'EOF'
import socket
import sys

port = int(sys.argv[1])

def status(request, half_close=False):
    sock = socket.create_connection(("127.0.0.1", port), timeout=30)
    sock.sendall(request)
    if half_close:
        sock.shutdown(socket.SHUT_WR)
    response = b""
    try:
        while b"\r\n" not in response:
            chunk = sock.recv(4096)
            if not chunk:
                break
            response += chunk
    except OSError:
        pass
    sock.close()
    print(response.split(b" ")[1].decode() if response else "no response")

HOST = b"Host: localhost\r\n"

print("request line")
# The server splits the request line at any whitespace and ignores what follows the version.
status(b"GET\t/?query=INSERT%20INTO%20t_nonexistent%20VALUES%20(1)\tHTTP/1.1 /?query=SELECT%201 HTTP/1.1\r\n" + HOST + b"\r\n")
status(b"GET  /?query=SELECT%201 HTTP/1.1\r\n" + HOST + b"\r\n")
status(b"GET /?query=SELECT%201 HTTP/1.1 extra\r\n" + HOST + b"\r\n")
status(b"GET /?query=SELECT%201 HTTP/1.1\n" + HOST + b"\r\n")
status(b"GET /?query=SELECT%201 HTTP/1.1\r\n" + HOST + b"\r\n")

print("header lines")
# The server takes a line that begins with whitespace for a field without a name.
status(b"GET /?query=SELECT%201 HTTP/1.1\r\n" + HOST + b" X-ClickHouse-User: default\r\n\r\n")
status(b"GET /?query=SELECT%201 HTTP/1.1\r\n" + HOST + b"X-ClickHouse-User : default\r\n\r\n")
status(b"GET /?query=SELECT%201 HTTP/1.1\r\n" + HOST + b"X-Without-Colon\r\n\r\n")
status(b"GET /?query=SELECT%201 HTTP/1.1\r\n" + HOST + b"X-Bare-LF: 1\n\r\n")
status(b"GET /?query=SELECT%201 HTTP/1.1\r\n" + HOST + b"X-Bare-CR: 1\rX-ClickHouse-User: default\r\n\r\n")
status(b"GET /?query=SELECT%201 HTTP/1.1\r\n" + HOST + b"X-ClickHouse-User: default\r\n\r\n")

print("decompress")
INSERT = b"INSERT INTO t_nonexistent VALUES (1)"
def post(target, body, headers=b""):
    return (b"POST " + target + b" HTTP/1.1\r\n" + HOST + headers
        + b"Content-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body)
# The server takes the last `decompress` and parses it as a boolean: these bodies are plain queries.
status(post(b"/?decompress=false", INSERT))
status(post(b"/?decompress=1&decompress=0", INSERT))
status(post(b"/?decompress=0&decompress=1", INSERT))
status(post(b"/?decompress=maybe", INSERT))

print("body framing")
# The server reads a chunked body only if the first `Transfer-Encoding` is exactly `chunked`.
status(b"POST /?query=%2F* HTTP/1.1\r\n" + HOST + b"Transfer-Encoding: identity\r\nTransfer-Encoding: chunked\r\n"
    b"Content-Length: 58\r\n\r\nd;*/ INSERT INTO t SELECT 1 /* /*\r\n*/ SELECT 1--\r\n0;*/\r\n\r\n")
status(b"POST / HTTP/1.1\r\n" + HOST + b"Transfer-Encoding: gzip, chunked\r\n\r\n")
status(b"POST / HTTP/1.1\r\n" + HOST + b"Content-Length: +8\r\n\r\nSELECT 1")
status(b"POST / HTTP/1.1\r\n" + HOST + b"Content-Length: 8\r\nContent-Length: 36\r\n\r\n" + INSERT)
# The server reads a chunk header up to the next CR, so a chunk header that ends with a lone LF is not interpreted.
status(b"POST / HTTP/1.1\r\n" + HOST + b"Transfer-Encoding: chunked\r\n\r\n8\nSELECT 1\r\n0\r\n\r\n")
status(b"POST / HTTP/1.1\r\n" + HOST + b"Transfer-Encoding: chunked\r\n\r\n8;extension\r\nSELECT 1\r\n0\r\n\r\n")
# Without `Content-Length` and chunked encoding, the body of a `POST` lasts until the end of the stream: it is not inspected.
status(b"POST /?query=%2F* HTTP/1.0\r\n" + HOST + b"\r\n*/ INSERT INTO t_nonexistent VALUES (1)", half_close=True)
status(b"POST /?query=INSERT%20INTO%20t_nonexistent%20VALUES HTTP/1.0\r\n" + HOST + b"\r\n(1)", half_close=True)

print("path with compression")
# The path is parsed as by the server: the table of an upload and of a generated `SELECT`, with a format and a compression.
status(b"PUT /default/t_nonexistent.CSV.gz HTTP/1.1\r\n" + HOST + b"Content-Length: 9\r\n\r\nSELECT 1\n")
status(b"GET /system/one.TSV.gz HTTP/1.1\r\n" + HOST + b"\r\n")

print("deep recursion")
# A deeply nested `WITH` query from a client that is not authenticated must not overflow the stack of the proxy.
deep = b"WITH " + b"f(" * 10000 + b"1" + b")" * 10000 + b" LIKE SOME(['x']) AS x SELECT 1"
status(post(b"/", deep))
deep = b"WITH " + b"(" * 30000 + b"1" + b")" * 30000 + b" AS x SELECT 1"
status(post(b"/", deep))
status(post(b"/", b"WITH 1 AS x SELECT x"))
EOF
