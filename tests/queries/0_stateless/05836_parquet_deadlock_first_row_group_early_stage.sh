#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With a tiny `input_format_parquet_memory_high_watermark`, a later row group could get through the
# bloom filter and dictionary stages ahead of the first incomplete one and hold the `Dictionary` stage's
# memory. The first incomplete row group then waited for that memory, while the later row group waited
# for it to be delivered, and the reader threw `Deadlock in Parquet::ReadManager`.

FILE="${CLICKHOUSE_DATABASE}/t_parquet_deadlock_first_row_group.parquet"

# Thirty row groups and a few concurrent queries keep the test cheap; it is a smoke test of this
# scheduling path, the race itself is rare at this size.
$CLICKHOUSE_CLIENT -q "
INSERT INTO FUNCTION file('$FILE', Parquet, 'id UInt64, s String, u String, v String')
SELECT number, toString(number % 100), toString(intDiv(number, 7) % 50), toString(number % 13)
FROM numbers(3000)
SETTINGS engine_file_truncate_on_insert = 1,
    output_format_parquet_row_group_size = 100,
    output_format_parquet_row_group_size_bytes = 536870912,
    output_format_parquet_write_bloom_filter = 1,
    output_format_parquet_max_dictionary_size = 1048576;
"

query()
{
    local i=$1
    $CLICKHOUSE_CLIENT -q "
    SELECT count(), sum(cityHash64(s)) FROM file('$FILE', Parquet)
    PREWHERE s = '12' OR u = '7' OR v = '3'
    SETTINGS input_format_parquet_memory_low_watermark = $((i % 2)),
        input_format_parquet_memory_high_watermark = 1,
        input_format_parquet_filter_push_down = 1,
        input_format_parquet_bloom_filter_push_down = 1,
        input_format_parquet_dictionary_filter_push_down = 1048576,
        input_format_parquet_preserve_order = 0,
        max_threads = 1,
        max_parsing_threads = 16;
    "
}

for i in {1..4}; do
    query "$i" &
done | sort | uniq -c | sed 's/^ *//'
