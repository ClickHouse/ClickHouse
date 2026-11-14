#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings
# no-fasttest: Parquet support is not built in fast-test images
# no-parallel: the columns cache is system-wide, and the test drops it and checks what it holds
# no-random-settings: the memory limit depends on the settings

# A row group of a `Parquet` file that uses the columns cache is read in chunks of 65536 rows, unless a whole chunk would
# take a large part of the memory limit of the query while the settings ask for smaller blocks. The size of a chunk must
# not be underestimated when the values differ in size a lot, and estimating it must not read much. Each query below
# succeeds with the cache as it does without it:
# - a chunk to write to the cache, of strings encoded with `DELTA_BYTE_ARRAY` without the length in the metadata, which
#   take much less in the file than decoded;
# - a chunk to write to the cache, of short strings converted to a much wider type;
# - a chunk to write to the cache that holds all the long strings, or all the non-null values, of the row group;
# - a chunk to write to the cache, with a wide column that is missing in the file and is filled with default values;
# - a chunk to write to the cache, with arrays of tuples of which all the requested elements are missing in the file.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./05448_parquet_columns_cache_skewed_sizes.lib
. "$CUR_DIR"/05448_parquet_columns_cache_skewed_sizes.lib

echo "--- strings encoded with DELTA_BYTE_ARRAY without size statistics: not cached"
write_with_pyarrow delta
run_query "SELECT count(), sum(length(s)) FROM file('${FILE}', Parquet, 's String')
    SETTINGS max_threads = 1, input_format_parquet_max_block_size = 1000, input_format_parquet_memory_high_watermark = '4Mi',
    max_memory_usage = '40Mi'"

echo "--- short strings read as a wide FixedString: not cached"
write_with_pyarrow short_strings
run_query "SELECT count(), sum(cityHash64(s)) FROM file('${FILE}', Parquet, 's FixedString(256)')
    SETTINGS max_threads = 1, input_format_parquet_max_block_size = 1000, input_format_parquet_memory_high_watermark = '4Mi',
    max_memory_usage = '12Mi'"

# One row group of 4 chunks, read cold: the strings of the first chunk take 32 MiB, the others are empty or null.
function write_skewed_row_group()
{
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO FUNCTION file('${FILE}.tmp', Parquet, 's $1')
        SELECT if(number < 65536, repeat('x', 512), $2) FROM numbers(262144)
        SETTINGS max_block_size = 262144, output_format_parquet_row_group_size = 10000000, output_format_parquet_row_group_size_bytes = '1Gi',
        engine_file_truncate_on_insert = 1"
    if [ "$3" = strip ]; then
        python3 "${CUR_DIR}/helpers/parquet_strip_size_statistics.py" "${FILE_PATH}.tmp" "${FILE_PATH}"
        rm -f "${FILE_PATH}.tmp"
    else
        mv "${FILE_PATH}.tmp" "${FILE_PATH}"
    fi
    touch -d "1 minute ago" "${FILE_PATH}"
    run_query "SELECT count(), sum(length(s)) FROM file('${FILE}', Parquet, 's $1')
        SETTINGS max_threads = 1, input_format_parquet_max_block_size = 1000, input_format_parquet_memory_high_watermark = '4Mi',
        max_memory_usage = '40Mi'"
}

echo "--- the long strings of a row group in one chunk, with size statistics"
write_skewed_row_group String "''" keep

echo "--- the non-null strings of a row group in one chunk, without size statistics"
write_skewed_row_group "Nullable(String)" NULL strip

echo "--- a wide column missing in the file: not cached"
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('${FILE}', Parquet) SELECT number AS id FROM numbers(65536)
    SETTINGS engine_file_truncate_on_insert = 1"
touch -d "1 minute ago" "${FILE_PATH}"
run_query "SELECT sum(id), sum(cityHash64(missing)) FROM file('${FILE}', Parquet, 'id UInt64, missing FixedString(256)')
    SETTINGS max_threads = 1, input_format_parquet_max_block_size = 1000, input_format_parquet_memory_high_watermark = '4Mi',
    max_memory_usage = '12Mi'"

echo "--- arrays of tuples of which all the requested elements are missing in the file: not cached"
# The arrays are empty, but their offsets take 8 bytes per row: 16 MiB in 32 columns.
ARRAYS=$(seq -s ', ' -f 'a%g' 0 31)
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('${FILE}', Parquet, 'id UInt64, $(seq -s ', ' -f 'a%g Array(Tuple(old UInt8))' 0 31)')
    SELECT number$(printf ', []%.0s' $(seq 32)) FROM numbers(65536)
    SETTINGS engine_file_truncate_on_insert = 1"
touch -d "1 minute ago" "${FILE_PATH}"
run_query "SELECT sum(id), sum(cityHash64(tuple(${ARRAYS})))
    FROM file('${FILE}', Parquet, 'id UInt64, $(seq -s ', ' -f 'a%g Array(Tuple(missing UInt8))' 0 31)')
    SETTINGS max_threads = 1, input_format_parquet_max_block_size = 1000, input_format_parquet_memory_low_watermark = 0,
    input_format_parquet_memory_high_watermark = '4Mi', max_memory_usage = '12Mi'"

rm -f "${FILE_PATH}" "${FILE_PATH}.tmp"
