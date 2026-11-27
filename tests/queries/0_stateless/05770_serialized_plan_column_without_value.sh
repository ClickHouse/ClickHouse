#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# We should have correct env vars from shell_config.sh to run this test
python3 "$CUR_DIR"/05770_serialized_plan_column_without_value.python

# The server is still alive.
$CLICKHOUSE_CLIENT -q "SELECT 'server alive', sum(number) FROM numbers(10)"
