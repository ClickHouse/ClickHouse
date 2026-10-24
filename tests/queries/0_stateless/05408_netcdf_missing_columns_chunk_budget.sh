#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE=${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_missing_budget.nc

$CLICKHOUSE_LOCAL -q "SELECT number::Int32 AS a FROM numbers(65536) INTO OUTFILE '$FILE' FORMAT NetCDF"

# The default values of a column that the file lacks count toward the size of a chunk, so the chunk
# stays within `input_format_max_block_size_bytes` even when they are wide.
$CLICKHOUSE_LOCAL -q "SELECT length(c) FROM file('$FILE', NetCDF, 'a Int32, c FixedString(256)') LIMIT 1 SETTINGS input_format_max_block_size_bytes = 1048576, max_memory_usage = 8388608"

rm -f "$FILE"
