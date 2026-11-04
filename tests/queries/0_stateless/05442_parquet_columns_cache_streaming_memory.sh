#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings
# no-fasttest: Parquet support is not built in fast-test images
# no-parallel: the columns cache is system-wide, and the test drops it and checks what it holds
# no-random-settings: the memory limit depends on the settings

# A row group written to the columns cache is not held by the query until the row group is read: each chunk of
# 65536 rows is written to the cache as it is decoded, and its memory is the server's from then on. So a query that
# populates the cache needs memory for about a chunk, not for the decoded row group, and fits a memory limit below the
# size of the row group. The chunks of a column that turns out not to be cacheable as a whole are taken back.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
FILE_PATH="${USER_FILES_PATH}/${FILE}"

# One row group of 8 chunks. The strings are dictionary-encoded, so the file is small, but decoded, the column `s`
# takes about 38 MiB, and a chunk about 5 MiB.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('${FILE}', Parquet)
    SELECT number AS id, leftPad(toString(number % 16), 64, 'x') AS s FROM numbers(524288)
    SETTINGS output_format_parquet_row_group_size = 1000000, engine_file_truncate_on_insert = 1"
$CLICKHOUSE_CLIENT -q "SELECT num_row_groups, num_rows FROM file('${FILE}', ParquetMetadata)"
# Only a file that has not been modified for a few seconds is cached.
touch -d "1 minute ago" "${FILE_PATH}"

CACHE="use_columns_cache = 1"
QUERY="SELECT count(), sum(length(s)) FROM file('${FILE}', Parquet, 'id UInt64, s String') SETTINGS max_threads = 1"
# Below the decoded row group. The peak memory usage of a read without the cache varies with the block sizes it decodes,
# so only the reads with the cache are limited.
LIMIT="max_memory_usage = '32Mi'"
ENTRIES="SELECT column, count(), sum(rows) FROM system.columns_cache WHERE database = '' AND endsWith(part, '/${FILE}') GROUP BY column ORDER BY column"

echo "--- without the cache"
$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"
$CLICKHOUSE_CLIENT -q "${QUERY}, use_columns_cache = 0"

echo "--- a memory limit below the decoded row group, with a cold cache: all chunks are written"
$CLICKHOUSE_CLIENT -q "${QUERY}, ${LIMIT}, ${CACHE}"
$CLICKHOUSE_CLIENT -q "${ENTRIES}"

echo "--- the same limit, served from the cache"
$CLICKHOUSE_CLIENT -q "${QUERY}, ${LIMIT}, ${CACHE}"

echo "--- the chunks of a column that cannot be written in full are taken back"
# The first four chunks of `s` pass `PREWHERE` in full and are written, the next ones are filtered. The chunks of `id`,
# the column of the filter, are all read in full: the filter cannot skip any page by the column index.
$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(id), sum(length(s)) FROM file('${FILE}', Parquet, 'id UInt64, s String') PREWHERE id < 262144 OR id % 2 = 0 SETTINGS max_threads = 1, ${CACHE}"
$CLICKHOUSE_CLIENT -q "${ENTRIES}"

rm -f "${FILE_PATH}"
