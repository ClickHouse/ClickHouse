#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# 8 rows: `keep` (INT32, PLAIN) is 1, 0, 1, 0, ... and `value` (INT32 with converted type INT_8,
# DELTA_BINARY_PACKED) is 1, 2, ..., 8. INT_8 makes the converter of `value` not trivial.
# With small blocks, the later subgroups of the page apply the PREWHERE filter inside the decoder,
# which used to decode the filtered-out values into the same buffer as the kept ones and overwrite them.
for block_size in 1 2 3 4 5 8
do
    ${CLICKHOUSE_LOCAL} --query "
        SELECT arraySort(groupArray(value)) FROM file('$CUR_DIR/data_parquet/05479_delta_binary_packed_filter_int8.parquet')
        PREWHERE keep = 1
        SETTINGS
            input_format_parquet_max_block_size = $block_size,
            input_format_parquet_use_offset_index = 0,
            input_format_parquet_filter_push_down = 0,
            input_format_parquet_page_filter_push_down = 0,
            input_format_parquet_bloom_filter_push_down = 0"
done
