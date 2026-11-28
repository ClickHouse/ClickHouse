#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings
# no-fasttest: Parquet support is not built in fast-test images
# no-parallel: the columns cache is system-wide, and the test drops it and checks what it holds
# no-random-settings: the memory limit depends on the settings

# A row group of a `Parquet` file that uses the columns cache is read in chunks of 65536 rows, unless a whole chunk would
# take a large part of the memory limit of the query while the settings ask for smaller blocks. The size of a chunk must
# not be underestimated when the values differ in size a lot, and estimating it must not read much. Each query below
# succeeds with the cache as it does without it:
# - a column to write to the cache that `PREWHERE` does not need, with a small dictionary page, in each of several files
#   read in parallel, of which the dictionary pages do not fit the memory limit together;
# - a chunk served from the cache, in each of several files read in parallel, of which the chunks do not fit the memory
#   limit together.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./05448_parquet_columns_cache_skewed_sizes.lib
. "$CUR_DIR"/05448_parquet_columns_cache_skewed_sizes.lib

echo "--- small dictionary pages of a column that PREWHERE does not need, in files read in parallel"
# Each of 8 files has a dictionary page of 1.5 MiB, which fits the budget of one reader, but not of all of them together.
# The read buffers are small, so that the 8 files read at once fit the memory limit without the cache.
write_with_pyarrow parallel_files
for i in $(seq 0 7); do
    cp "${FILE_PATH}" "${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_parallel_${i}.parquet"
    touch -d "1 minute ago" "${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_parallel_${i}.parquet"
done
QUERY="SELECT count(), sum(length(s)) FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}_parallel_*.parquet', Parquet, 'id UInt64, s String')
    PREWHERE id % 2 = 1
    SETTINGS max_threads = 8, input_format_parquet_max_block_size = 1000, input_format_parquet_memory_low_watermark = 0,
    input_format_parquet_memory_high_watermark = '4Mi', max_memory_usage = '8Mi', max_untracked_memory = 65536,
    max_read_buffer_size = 65536, use_query_condition_cache = 0"
$CLICKHOUSE_CLIENT -q "
    SYSTEM DROP COLUMNS CACHE;
    ${QUERY}, use_columns_cache = 0;
    ${QUERY}, use_columns_cache = 1"
rm -f "${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"_parallel_*.parquet

echo "--- chunks served from the cache, in files read in parallel"
# Each of 8 files has a row group of 65536 rows of `FixedString(64)`, a chunk of 4 MiB, which fits the allowance of one
# reader, but not of all of them together, with the results of a function of the column.
for i in $(seq 0 7); do
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO FUNCTION file('${CLICKHOUSE_TEST_UNIQUE_NAME}_parallel_chunk_${i}.parquet', Parquet, 's FixedString(64)')
        SELECT toFixedString(leftPad(toString(number % 128), 64, 'x'), 64) FROM numbers(65536)
        SETTINGS engine_file_truncate_on_insert = 1"
    touch -d "1 minute ago" "${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}_parallel_chunk_${i}.parquet"
done
FILES="file('${CLICKHOUSE_TEST_UNIQUE_NAME}_parallel_chunk_*.parquet', Parquet, 's FixedString(64)')"
QUERY="SELECT count(), sum(cityHash64(concat(s, '!'))) FROM ${FILES}
    SETTINGS max_threads = 8, input_format_parquet_max_block_size = 1000, input_format_parquet_memory_low_watermark = 0,
    input_format_parquet_memory_high_watermark = '4Mi', max_memory_usage = '24Mi', max_untracked_memory = 65536,
    max_read_buffer_size = 65536"
$CLICKHOUSE_CLIENT -q "
    SYSTEM DROP COLUMNS CACHE;
    ${QUERY}, use_columns_cache = 0;
    SELECT count(), sum(cityHash64(s)) FROM ${FILES} SETTINGS use_columns_cache = 1;
    SELECT count(), sum(rows) FROM system.columns_cache WHERE database = '' AND part LIKE '%/${CLICKHOUSE_TEST_UNIQUE_NAME}_parallel_chunk_%';
    ${QUERY}, use_columns_cache = 1"
rm -f "${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}"_parallel_chunk_*.parquet

rm -f "${FILE_PATH}" "${FILE_PATH}.tmp"
