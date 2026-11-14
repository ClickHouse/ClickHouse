#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# We should have correct env vars from shell_config.sh to run this test
CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT" CLICKHOUSE_CLIENT_BINARY="$CLICKHOUSE_CLIENT_BINARY" \
    python3 "$CUR_DIR"/05618_detached_serialization_from_untrusted_server.python

$CLICKHOUSE_CLIENT -q "SELECT 'server alive'"
