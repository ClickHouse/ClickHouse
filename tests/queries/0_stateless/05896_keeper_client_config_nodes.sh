#!/usr/bin/env bash
# Tags: zookeeper

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TESTDIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_keeper_client_config_nodes"
trap 'rm -rf "$TESTDIR"' EXIT
rm -rf "$TESTDIR"
mkdir -p "$TESTDIR"

TIMEOUTS=(--connection-timeout 60 --session-timeout 60 --operation-timeout 60)

run_with_config()
{
    ${CLICKHOUSE_BINARY} keeper-client -c "$1" --log-level information "${TIMEOUTS[@]}" -q "exists /" 2>"$TESTDIR/stderr.log"
    grep -c -F "Found keeper node in $1" "$TESTDIR/stderr.log"
}

echo "-- every node is used, the first one refuses connections"
cat > "$TESTDIR/two_nodes.xml" <<XML
<clickhouse>
    <zookeeper>
        <node>
            <host>127.0.0.1</host>
            <port>1</port>
        </node>
        <node>
            <host>localhost</host>
            <port>${CLICKHOUSE_PORT_KEEPER}</port>
        </node>
    </zookeeper>
</clickhouse>
XML
run_with_config "$TESTDIR/two_nodes.xml"

echo "-- a section without nodes uses the default localhost:9181"
cat > "$TESTDIR/no_nodes.xml" <<XML
<clickhouse>
    <zookeeper>
        <session_timeout_ms>60000</session_timeout_ms>
    </zookeeper>
</clickhouse>
XML
run_with_config "$TESTDIR/no_nodes.xml"

echo "-- a node without port is accepted, secure 0 is a plaintext node"
cat > "$TESTDIR/default_port.xml" <<XML
<clickhouse>
    <zookeeper>
        <node>
            <host>127.0.0.1</host>
        </node>
        <node>
            <host>localhost</host>
            <port>${CLICKHOUSE_PORT_KEEPER}</port>
            <secure>0</secure>
        </node>
    </zookeeper>
</clickhouse>
XML
run_with_config "$TESTDIR/default_port.xml"

echo "-- the error names every node, a node without port uses 2181"
cat > "$TESTDIR/refused.xml" <<XML
<clickhouse>
    <zookeeper>
        <node>
            <host>127.0.0.1</host>
            <port>1</port>
        </node>
        <node>
            <host>127.0.0.1</host>
        </node>
    </zookeeper>
</clickhouse>
XML
${CLICKHOUSE_BINARY} keeper-client -c "$TESTDIR/refused.xml" "${TIMEOUTS[@]}" -q "exists /" 2>"$TESTDIR/stderr.log"
grep -o -E '127\.0\.0\.1:[0-9]+' "$TESTDIR/stderr.log" | LC_ALL=C sort -u
