#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Piping through head should terminate quietly without a broken pipe exception
output=$($CLICKHOUSE_CLIENT -q "SELECT * FROM numbers(1000000)" 2>&1 | head -1)
echo "$output"

output=$($CLICKHOUSE_LOCAL -q "SELECT * FROM numbers(1000000)" 2>&1 | head -3)
echo "$output"
