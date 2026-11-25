#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The native protocol can frame a TCP stream into length-prefixed chunks, negotiated through
# `proto_caps` in the handshake (docs/en/interfaces/tcp.md, `src/IO/WriteBufferFromPocoSocketChunked.h`).
# `tests/config` leaves `proto_caps` at the default `notchunked`, so queries against the ordinary test
# server never frame anything. This test brings up its own listener that *requires* framing and queries
# it from a second process over a cluster entry that asks for it.
#
# OS-assigned ports (`--tcp_port 0 --http_port 0`) keep this parallel-safe; the endpoint is built from
# the actually bound port via `getServerPort`.

work_dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_chunked"
rm -rf "$work_dir"
mkdir -p "$work_dir"

# `chunked` without the `_optional` suffix is the strict form: the listener refuses a client that asks
# for an unframed stream. That is what makes the successful queries below evidence that framing was in
# use, rather than a silent fallback that would assert the same numbers.
cat > "$work_dir/listener.xml" <<'EOF'
<clickhouse>
    <proto_caps>
        <send>chunked</send>
        <recv>chunked</recv>
    </proto_caps>
</clickhouse>
EOF

# Two pipes instead of a wait loop: the listener's stdin stays open for as long as this script holds
# the first one, and reading one line from the second blocks exactly until the port has been bound.
# Both are opened read-write here so that neither open() has to wait for its other end.
mkfifo "$work_dir/listener_stdin" "$work_dir/listener_stdout"
exec {listener_stdin}<>"$work_dir/listener_stdin"
exec {listener_stdout}<>"$work_dir/listener_stdout"

$CLICKHOUSE_LOCAL --config-file="$work_dir/listener.xml" --path "$work_dir/listener" \
    --listen_host 127.0.0.1 --tcp_port 0 --http_port 0 --interactive \
    --query "SYSTEM START LISTEN QUERIES ALL; SELECT getServerPort('tcp_port') FORMAT TSV" \
    < "$work_dir/listener_stdin" > "$work_dir/listener_stdout" 2>/dev/null &
listener_pid=$!

# Runs on every exit path, including the mandatory check below, so a failure never leaves the listener
# behind. Closing its stdin ends the session, which is the normal shutdown path.
cleanup() {
    exec {listener_stdin}>&-
    kill "$listener_pid" 2>/dev/null
    wait "$listener_pid" 2>/dev/null
    exec {listener_stdout}>&-
    rm -rf "$work_dir"
}
trap cleanup EXIT

read -t 60 -r tcp_port <&"$listener_stdout"
# Mandatory: without it the queries below would run against no listener at all and fail with an error
# that looks just like a protocol rejection.
[ -n "$tcp_port" ] || { echo "failed to read the listener port"; exit 1; }

# `proto_caps` on a cluster replica is what turns framing on for the initiator's outgoing connection
# (`src/Interpreters/Cluster.cpp`).
cat > "$work_dir/initiator.xml" <<EOF
<clickhouse>
    <remote_servers>
        <chunked_listener>
            <shard><replica>
                <host>127.0.0.1</host>
                <port>${tcp_port}</port>
                <proto_caps><send>chunked</send><recv>chunked</recv></proto_caps>
            </replica></shard>
        </chunked_listener>
    </remote_servers>
</clickhouse>
EOF

# The scalar subquery is the point of the first query: a scalar is pushed to the replica as its own
# protocol packet whose chunk the socket flushes immediately, and the chunk is closed only after the
# last scalar, so closing it has to turn an already-sent chunk header into an end-of-chunk marker. The
# second query closes chunks that are still buffered, which is the other half of the same function.
$CLICKHOUSE_LOCAL --config-file="$work_dir/initiator.xml" --path "$work_dir/initiator" --query "
    SELECT count() FROM clusterAllReplicas('chunked_listener', numbers(5)) WHERE number < (SELECT 3);
    SELECT sum(number) FROM clusterAllReplicas('chunked_listener', numbers(100000));
"

# The negative half of the proof: a client that does not ask for framing is turned away. Only the name
# of the error code is printed, not the message.
$CLICKHOUSE_CLIENT_BINARY --host 127.0.0.1 --port "$tcp_port" --query "SELECT 1" 2>&1 >/dev/null |
    grep -o -m1 NETWORK_ERROR
