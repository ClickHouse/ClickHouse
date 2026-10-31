#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The error names the function and the rejected value.
$CLICKHOUSE_CLIENT -q "SELECT ntile(0) OVER (ORDER BY number) FROM numbers(3)" 2>&1 | grep -o "'ntile' function must be in \[1, 9223372036854775807\], 0 given" | head -n 1
