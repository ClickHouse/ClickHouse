#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A single `DELTA_LENGTH_BYTE_ARRAY` page split across several output blocks:
# the offsets of the later blocks must be rebased without unsigned wrap-around.
${CLICKHOUSE_LOCAL} --query "
    SELECT s, length(s) FROM file('$CUR_DIR/data_parquet/delta_length_byte_array_split_page.parquet') ORDER BY s
    SETTINGS input_format_parquet_max_block_size = 1"
${CLICKHOUSE_LOCAL} --query "
    SELECT s FROM file('$CUR_DIR/data_parquet/delta_length_byte_array_split_page.parquet') ORDER BY s
    SETTINGS input_format_parquet_max_block_size = 3"
