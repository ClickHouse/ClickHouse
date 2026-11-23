#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings
# no-fasttest: Parquet support is not built in fast-test images
# no-parallel: the columns cache is system-wide, and the test drops it and checks what it holds
# no-random-settings: the memory limit depends on the settings

# A whole chunk of 65536 rows served from the columns cache takes no memory of the query, but the rows of a chunk that
# pass a filter are copied out of it. So a row group with a filter is read from the cache in the blocks the settings ask
# for when the copy of a whole chunk would take a large part of the memory limit of the query, as without the cache.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
FILE_PATH="${USER_FILES_PATH}/${FILE}"

# One row group of one chunk. The strings are dictionary-encoded, so the file is small, but decoded, the column `s`
# takes about 32 MiB.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('${FILE}', Parquet)
    SELECT number AS id, leftPad(toString(number % 16), 512, 'x') AS s FROM numbers(65536)
    SETTINGS output_format_parquet_row_group_size = 1000000, engine_file_truncate_on_insert = 1"
$CLICKHOUSE_CLIENT -q "SELECT num_row_groups, num_rows FROM file('${FILE}', ParquetMetadata)"
# Only a file that has not been modified for a few seconds is cached.
touch -d "1 minute ago" "${FILE_PATH}"

QUERY="SELECT count(), sum(length(s)) FROM file('${FILE}', Parquet, 'id UInt64, s String') PREWHERE id % 16 != 0
    SETTINGS max_threads = 1, input_format_parquet_max_block_size = 1000, input_format_parquet_memory_high_watermark = '4Mi',
    max_memory_usage = '24Mi'"

echo "--- without the cache"
$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"
$CLICKHOUSE_CLIENT -q "${QUERY}, use_columns_cache = 0"

echo "--- warm the cache"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(id), sum(length(s)) FROM file('${FILE}', Parquet, 'id UInt64, s String') SETTINGS max_threads = 1, use_columns_cache = 1"
$CLICKHOUSE_CLIENT -q "SELECT column, count(), sum(rows) FROM system.columns_cache WHERE database = '' AND endsWith(part, '/${FILE}') GROUP BY column ORDER BY column"

echo "--- the filtered rows of the cached chunk are copied in the blocks the settings ask for"
$CLICKHOUSE_CLIENT -q "${QUERY}, use_columns_cache = 1"

rm -f "${FILE_PATH}"
