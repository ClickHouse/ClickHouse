#!/usr/bin/env bash

# `PREWHERE` over a Parquet file with many small pages, where some pages lie entirely inside runs of
# filtered-out rows. Such pages are passed to the page decoders with an all-zero filter, and the decoders
# skip their values. Covers `PLAIN` `BYTE_ARRAY` read through a non-trivial string converter (GeoParquet
# WKB into `Point`). The values that pass must be decoded from the right positions. There is no page index,
# so the filter is applied inside the decoders.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

file="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

$CLICKHOUSE_LOCAL -q "
    SELECT number AS k, (toFloat64(number), toFloat64(-number))::Point AS g
    FROM numbers(20000)
    FORMAT Parquet
    SETTINGS output_format_parquet_geometadata = 1, output_format_parquet_compression_method = 'none',
        output_format_parquet_data_page_size = 1024, output_format_parquet_batch_size = 64,
        output_format_parquet_write_page_index = 0, output_format_parquet_max_dictionary_size = 0,
        output_format_parquet_write_bloom_filter = 0, output_format_parquet_row_group_size = 20000" > "$file"

S="input_format_parquet_max_block_size = 4096, max_threads = 1"
F="file('$file', 'Parquet', 'k UInt64, g Point')"

$CLICKHOUSE_LOCAL -q "SELECT count(), sum(g.1), sum(g.2), countIf(g != (toFloat64(k), toFloat64(-k))) FROM $F PREWHERE intDiv(k, 1000) % 2 = 0 SETTINGS $S"
$CLICKHOUSE_LOCAL -q "SELECT k, g FROM $F PREWHERE intDiv(k, 1000) % 2 = 0 AND k % 1000 IN (0, 999) ORDER BY k LIMIT 4 SETTINGS $S"
$CLICKHOUSE_LOCAL -q "SELECT count(), sum(g.1) FROM $F PREWHERE k % 3000 >= 2500 SETTINGS $S"

# Corrupt the length of the value for k = 5500, which lies in the middle of a range of filtered-out rows,
# so its page is fully filtered while other rows of the block pass (the filter is applied inside the
# decoders starting from the second block). Skipping the values of that page must detect the invalid
# length instead of reading out of bounds. Checksums are not written, so that the corruption is not
# caught by the page checksum.
corrupted="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_corrupted.parquet"
$CLICKHOUSE_LOCAL -q "
    SELECT number AS k, (toFloat64(number), toFloat64(-number))::Point AS g
    FROM numbers(20000)
    FORMAT Parquet
    SETTINGS output_format_parquet_geometadata = 1, output_format_parquet_compression_method = 'none',
        output_format_parquet_data_page_size = 1024, output_format_parquet_batch_size = 64,
        output_format_parquet_write_page_index = 0, output_format_parquet_max_dictionary_size = 0,
        output_format_parquet_write_bloom_filter = 0, output_format_parquet_row_group_size = 20000,
        output_format_parquet_write_checksums = 0" > "$file"
$CLICKHOUSE_LOCAL -q "
    WITH concat(unhex('0101000000'), reinterpretAsFixedString(toFloat64(5500)), reinterpretAsFixedString(toFloat64(-5500))) AS v
    SELECT replaceOne(raw, concat(unhex('15000000'), v), concat(unhex('FFFFFF7F'), v))
    FROM file('$file', 'RawBLOB', 'raw String')
    FORMAT RawBLOB" > "$corrupted"
cmp -s "$file" "$corrupted" && echo "the file was not corrupted"
F="file('$corrupted', 'Parquet', 'k UInt64, g Point')"
$CLICKHOUSE_LOCAL -q "SELECT count(), sum(g.1) FROM $F PREWHERE intDiv(k, 1000) % 2 = 0 SETTINGS $S" 2>&1 | grep -o -m1 'INCORRECT_DATA'

rm -f "$corrupted"

rm -f "$file"
