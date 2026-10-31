#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings
# no-fasttest: Parquet support is not built in fast-test images
# no-parallel: the columns cache is system-wide, and the test drops it and checks what it holds
# no-random-settings: the memory limit and the sizes of the chunks depend on the settings

# The columns cache holds a row group of a `Parquet` file in chunks of 65536 rows, and a chunk is written to it as
# decoded, which needs the whole chunk in the memory of the query. Too wide a chunk is not written:
# - one that would take a large part of the memory limit of the query: the row group is read in the blocks the settings
#   ask for, as without the cache;
# - one that the cache rejects, being larger than the cache: then no other chunk of the column is kept either, since
#   the column is served from the cache only with all its chunks.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
FILE_PATH="${USER_FILES_PATH}/${FILE}"
ENTRIES="SELECT column, count(), sum(rows) FROM system.columns_cache WHERE database = '' AND endsWith(part, '/${FILE}') GROUP BY column ORDER BY column"

# Write the file, one row group of $1 rows with strings of $2 bytes: dictionary-encoded, so the file is small.
function write_file()
{
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO FUNCTION file('${FILE}', Parquet)
        SELECT number AS id, leftPad(toString(number % 16), $2, 'x') AS s FROM numbers($1)
        SETTINGS output_format_parquet_row_group_size = 1000000, engine_file_truncate_on_insert = 1"
    $CLICKHOUSE_CLIENT -q "SELECT num_row_groups, num_rows FROM file('${FILE}', ParquetMetadata)"
    # Only a file that has not been modified for a few seconds is cached.
    touch -d "1 minute ago" "${FILE_PATH}"
    $CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"
}

QUERY="SELECT count(), sum(length(s)) FROM file('${FILE}', Parquet, 'id UInt64, s String') SETTINGS max_threads = 1"

echo "--- a chunk of 128 MiB, under a memory limit of 160 MiB: read in small blocks, not cached"
write_file 65536 2048
# Decoded whole, the chunk would take 256 MiB with the growth of its buffer. Read in small blocks, the row group takes as
# much memory as the reader decodes ahead of the blocks it delivers, which is bounded by the watermark rather than by the
# memory limit of the query, so the watermark is set low.
$CLICKHOUSE_CLIENT -q "${QUERY}, input_format_parquet_max_block_size = 1000, use_columns_cache = 0"
$CLICKHOUSE_CLIENT -q "${QUERY}, input_format_parquet_max_block_size = 1000, input_format_parquet_memory_high_watermark = '16Mi', max_memory_usage = '160Mi', use_columns_cache = 1"
$CLICKHOUSE_CLIENT -q "${ENTRIES}"

echo "--- a chunk larger than the cache of 100 MiB, and a short one: neither is kept"
write_file 66536 1700
$CLICKHOUSE_CLIENT -q "${QUERY}, max_memory_usage = '2Gi', use_columns_cache = 1"
$CLICKHOUSE_CLIENT -q "${ENTRIES}"

rm -f "${FILE_PATH}"
