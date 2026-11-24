#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The client derives its default port from `tcp_port` and `port_offset` of its configuration. Without
# `--port` / `--secure` it also considers the secure port, which is not configured here, so its
# built-in default shifted by the offset is far below zero. It is not a candidate at all then, and
# it must not make the client reject a configuration that is valid for the plain port.

CONFIG="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.xml"
trap 'rm -f "$CONFIG"' EXIT

OFFSET=50000
cat > "$CONFIG" <<EOF
<config>
    <tcp_port>$((CLICKHOUSE_PORT_TCP + OFFSET))</tcp_port>
    <port_offset>-${OFFSET}</port_offset>
</config>
EOF

${CLICKHOUSE_CLIENT_BINARY} --config-file "$CONFIG" --host "${CLICKHOUSE_HOST}" --query "SELECT 1"

# A configured secure port is required to be valid, though.
cat > "$CONFIG" <<EOF
<config>
    <tcp_port>$((CLICKHOUSE_PORT_TCP + OFFSET))</tcp_port>
    <tcp_port_secure>9440</tcp_port_secure>
    <port_offset>-${OFFSET}</port_offset>
</config>
EOF

${CLICKHOUSE_CLIENT_BINARY} --config-file "$CONFIG" --host "${CLICKHOUSE_HOST}" --query "SELECT 1" 2>&1 | grep -o -m1 "ARGUMENT_OUT_OF_BOUND"
