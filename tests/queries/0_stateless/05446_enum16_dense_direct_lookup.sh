#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A dense `Enum16` with more than 1024 values and the value -31768 missing.
type="Enum16($(seq 0 1999 | awk '$1 != 1000 { printf "%s'\''v%d'\'' = %d", (NR > 1 ? ", " : ""), $1, $1 - 32768 }'))"
$CLICKHOUSE_CLIENT --query "SELECT toString(CAST(-32768 AS $type)), toString(CAST(-30769 AS $type))"
$CLICKHOUSE_CLIENT --query "SELECT CAST(-31768 AS $type)" 2>&1 | grep -o -m1 UNKNOWN_ELEMENT_OF_ENUM

# An `Enum16` with all 65536 values does not fit the 16-bit indexes of the direct lookup.
type="Enum16($(seq 0 65535 | awk '{ printf "%s'\''v%d'\'' = %d", (NR > 1 ? ", " : ""), $1, $1 - 32768 }'))"
echo "SELECT toString(CAST(-32768 AS $type)), toString(CAST(32767 AS $type))" | $CLICKHOUSE_CLIENT --max_query_size 10000000
