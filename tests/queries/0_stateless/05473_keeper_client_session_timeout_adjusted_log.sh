#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The test keeper is configured with min_session_timeout_ms=10000
# (tests/config/config.d/keeper_port.xml). Requesting a smaller session
# timeout makes the server clamp it, and the client must report the
# adjustment instead of silently adopting the server value.
$CLICKHOUSE_BINARY keeper-client --port "$CLICKHOUSE_PORT_KEEPER" --connection-timeout 60 --session-timeout 1 --log-level=information -q "ls /" 2>&1 | grep -o "Server adjusted requested session timeout from 1000 ms to 10000 ms"
