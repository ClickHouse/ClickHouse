#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings
# no-fasttest: Parquet support is not built in fast-test images
# no-parallel: the columns cache is system-wide, and the test drops it and checks what it holds
# no-random-settings: the block sizes depend on the settings

# The columns cache holds a row group of a `Parquet` file in chunks of 65536 rows. A row group that uses
# the cache is read in its chunks, whatever the block size settings: a chunk decoded whole is written to
# the cache as it is, and a chunk found in the cache is the block itself.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
FILE_PATH="${USER_FILES_PATH}/${FILE}"

# One row group of 250000 rows: three whole chunks and a shorter fourth one.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('${FILE}', Parquet)
    SELECT number AS id, toString(number) AS s FROM numbers(250000)
    SETTINGS output_format_parquet_row_group_size = 1000000, engine_file_truncate_on_insert = 1"
$CLICKHOUSE_CLIENT -q "SELECT num_row_groups, num_rows FROM file('${FILE}', ParquetMetadata)"
# Only a file that has not been modified for a few seconds is cached.
touch -d "1 minute ago" "${FILE_PATH}"
$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"

SETTINGS="max_threads = 1, input_format_parquet_max_block_size = 1000"
QUERY="SELECT count(), sum(id), sum(length(s)) FROM file('${FILE}')"
BLOCKS="SELECT uniqExact(blockNumber()), max(blockSize()), min(blockSize()) FROM file('${FILE}', Parquet, 'id UInt64') SETTINGS ${SETTINGS}"

echo "--- without the cache: blocks of the size the settings ask for"
$CLICKHOUSE_CLIENT -q "${QUERY} SETTINGS ${SETTINGS}, use_columns_cache = 0"
$CLICKHOUSE_CLIENT -q "${BLOCKS}, use_columns_cache = 0"

echo "--- the first read writes the chunks"
$CLICKHOUSE_CLIENT -q "${QUERY} SETTINGS ${SETTINGS}, use_columns_cache = 1"
$CLICKHOUSE_CLIENT -q "
    SELECT column, count(), sum(rows), min(rows), max(rows), min(row_begin), max(row_end)
    FROM system.columns_cache
    WHERE database = '' AND endsWith(part, '/${FILE}')
    GROUP BY column ORDER BY column"

echo "--- the second read is served from the cache, in blocks of the chunks"
$CLICKHOUSE_CLIENT -q "${QUERY} SETTINGS ${SETTINGS}, use_columns_cache = 1"
$CLICKHOUSE_CLIENT -q "${BLOCKS}, use_columns_cache = 1"

echo "--- PREWHERE filters the rows served from the cache"
PREWHERE_QUERY="SELECT count(), sum(id), sum(length(s)) FROM file('${FILE}') PREWHERE id % 7 = 0 SETTINGS ${SETTINGS}"
$CLICKHOUSE_CLIENT -q "${PREWHERE_QUERY}, use_columns_cache = 0"
$CLICKHOUSE_CLIENT -q "${PREWHERE_QUERY}, use_columns_cache = 1"

echo "--- a part of the rows, from the middle of a chunk"
RANGE_QUERY="SELECT count(), sum(id), sum(length(s)) FROM file('${FILE}') WHERE id >= 200000 SETTINGS ${SETTINGS}"
$CLICKHOUSE_CLIENT -q "${RANGE_QUERY}, use_columns_cache = 0"
$CLICKHOUSE_CLIENT -q "${RANGE_QUERY}, use_columns_cache = 1"

rm -f "${FILE_PATH}"
