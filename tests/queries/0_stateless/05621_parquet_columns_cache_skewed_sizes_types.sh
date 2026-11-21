#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings
# no-fasttest: Parquet support is not built in fast-test images
# no-parallel: the columns cache is system-wide, and the test drops it and checks what it holds
# no-random-settings: the memory limit depends on the settings

# A row group of a `Parquet` file that uses the columns cache is read in chunks of 65536 rows, unless a whole chunk would
# take a large part of the memory limit of the query while the settings ask for smaller blocks. The size of a chunk must
# not be underestimated when the values differ in size a lot, and estimating it must not read much. Each query below
# succeeds with the cache as it does without it:
# - a chunk to write to the cache, of a variant read as `JSON`, which takes much more memory than the binary columns of
#   the variant in the file;
# - a chunk to write to the cache, of GeoParquet strings decoded into a geo type, which takes much more than the strings;
# - a chunk served from the cache, of `LowCardinality` strings, or arrays of them, which take little in the cache, but
#   much more converted to strings;
# - a chunk served from the cache, of `JSON`, which takes little in the cache, but much more as text, where each row
#   repeats the names of the paths.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./05448_parquet_columns_cache_skewed_sizes.lib
. "$CUR_DIR"/05448_parquet_columns_cache_skewed_sizes.lib

echo "--- a variant read as JSON: not cached"
# 65536 rows of an unshredded variant, recognized by its structure: objects of one field, of 64 distinct names, with
# a value of type `Int8`. Each name is a path of `JSON`, which takes a value in every row.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('${FILE}', Parquet, 'v Tuple(metadata String, value String)')
    SELECT tuple(concat(unhex('01010005'), 'k', leftPad(toString(number % 64), 4, '0')), unhex('02010000020C05')) FROM numbers(65536)
    SETTINGS output_format_parquet_string_as_string = 0, engine_file_truncate_on_insert = 1"
touch -d "1 minute ago" "${FILE_PATH}"
run_query "SELECT count(), sum(length(toJSONString(v))) FROM file('${FILE}', Parquet, 'v JSON')
    SETTINGS max_threads = 1, input_format_parquet_max_block_size = 1000, input_format_parquet_memory_low_watermark = 0,
    input_format_parquet_memory_high_watermark = '256Ki', max_memory_usage = '24Mi'"

echo "--- GeoParquet strings decoded into a much larger geo type: not cached"
# 65536 rows: the same `LineString` of 64 points as WKT of 267 bytes, with size statistics, whose coordinates take 1 KiB
# decoded.
python3 - "${FILE_PATH}" <<'EOF'
import sys
import pyarrow as pa
import pyarrow.parquet as pq

wkt = 'LINESTRING(' + ','.join(['0 0'] * 64) + ')'
geo = '{"version": "1.0.0", "primary_column": "g", "columns": {"g": {"encoding": "WKT", "geometry_types": ["LineString"]}}}'
table = pa.table({'g': pa.array([wkt] * 65536)}).replace_schema_metadata({'geo': geo})
pq.write_table(table, sys.argv[1], row_group_size=1000000, use_dictionary=True)
EOF
touch -d "1 minute ago" "${FILE_PATH}"
run_query "SELECT count(), sum(length(g)) FROM file('${FILE}', Parquet, 'g LineString')
    SETTINGS max_threads = 1, input_format_parquet_max_block_size = 1000, input_format_parquet_memory_low_watermark = 0,
    input_format_parquet_memory_high_watermark = '256Ki', max_memory_usage = '72Mi'"

echo "--- chunks of LowCardinality strings served from the cache, converted to strings"
# 65536 rows: the same string of 2 KiB, which takes 128 MiB expanded, but only a little as `LowCardinality`.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('${FILE}', Parquet, 's String, a Array(String)')
    SELECT repeat('x', 2048), [repeat('x', 2048)] FROM numbers(65536)
    SETTINGS engine_file_truncate_on_insert = 1"
touch -d "1 minute ago" "${FILE_PATH}"
STRUCTURE="'s LowCardinality(String), a Array(LowCardinality(String))'"
SETTINGS="max_threads = 1, input_format_parquet_max_block_size = 1000, input_format_parquet_memory_low_watermark = 0,
    input_format_parquet_memory_high_watermark = '4Mi', max_memory_usage = '64Mi', max_untracked_memory = 65536"
$CLICKHOUSE_CLIENT -q "
    SYSTEM DROP COLUMNS CACHE;
    SELECT count(), sum(length(s)), sum(length(a)) FROM file('${FILE}', Parquet, ${STRUCTURE}) SETTINGS max_threads = 1, use_columns_cache = 1;
    ${ENTRIES};
    SELECT count(), sum(cityHash64(CAST(s, 'String'))) FROM file('${FILE}', Parquet, ${STRUCTURE}) SETTINGS ${SETTINGS}, use_columns_cache = 1;
    SELECT count(), sum(cityHash64(CAST(a, 'Array(String)'))) FROM file('${FILE}', Parquet, ${STRUCTURE}) SETTINGS ${SETTINGS}, use_columns_cache = 1"

echo "--- chunks of JSON served from the cache, converted to text"
# 65536 rows of an unshredded variant: objects of one field, of the same name of 1 KiB, with a value of type `Int8`.
# `JSON` holds the name once, but `toJSONString` repeats it in every row, 64 MiB in total.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('${FILE}', Parquet, 'v Tuple(metadata String, value String)')
    SELECT tuple(concat(unhex('41010000000004'), repeat('k', 1024)), unhex('02010000020C05')) FROM numbers(65536)
    SETTINGS output_format_parquet_string_as_string = 0, engine_file_truncate_on_insert = 1"
touch -d "1 minute ago" "${FILE_PATH}"
SETTINGS="max_threads = 1, input_format_parquet_max_block_size = 1000, input_format_parquet_prefer_block_bytes = 0,
    input_format_parquet_memory_low_watermark = 0, input_format_parquet_memory_high_watermark = '4Mi',
    max_memory_usage = '48Mi', max_untracked_memory = 65536"
QUERY="SELECT count(), sum(length(toJSONString(v))) FROM file('${FILE}', Parquet, 'v JSON')"
$CLICKHOUSE_CLIENT -q "
    SYSTEM DROP COLUMNS CACHE;
    ${QUERY} SETTINGS ${SETTINGS}, use_columns_cache = 0;
    ${QUERY} SETTINGS max_threads = 1, input_format_parquet_max_block_size = 65536, input_format_parquet_prefer_block_bytes = 0,
        use_columns_cache = 1;
    ${ENTRIES};
    ${QUERY} SETTINGS ${SETTINGS}, use_columns_cache = 1"

rm -f "${FILE_PATH}" "${FILE_PATH}.tmp"
