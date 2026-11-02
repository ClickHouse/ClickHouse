#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings
# no-fasttest: Parquet support is not built in fast-test images
# no-parallel: the columns cache is system-wide, and the test drops it and checks what it holds
# no-random-settings: the memory limit depends on the settings

# A row group of a `Parquet` file that uses the columns cache is read in chunks of 65536 rows, unless a whole chunk would
# take a large part of the memory limit of the query while the settings ask for smaller blocks. Each query below succeeds
# with the cache as it does without it:
# - a chunk to write to the cache, of dictionary-encoded strings, the length of which is not in the metadata: it is
#   estimated by the dictionary, decoded before the row group is split into blocks, and a narrow chunk is still cached;
# - a chunk served from the cache, of which the column index leaves only a part of the rows, which are copied;
# - a chunk served from the cache whole, on which the query computes a function.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
FILE_PATH="${USER_FILES_PATH}/${FILE}"
ENTRIES="SELECT column, count(), sum(rows) FROM system.columns_cache WHERE database = '' AND endsWith(part, '/${FILE}') GROUP BY column ORDER BY column"

echo "--- dictionary-encoded strings of 64 bytes without size statistics, written by PyArrow: cached"
cp "${CUR_DIR}/data_parquet/dictionary_short_strings_without_size_statistics.parquet" "${FILE_PATH}"
# Only a file that has not been modified for a few seconds is cached.
touch -d "1 minute ago" "${FILE_PATH}"
$CLICKHOUSE_CLIENT -q "SELECT rg.columns.have_size_statistics FROM file('${FILE}', ParquetMetadata) ARRAY JOIN row_groups AS rg"
$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"
QUERY="SELECT count(), sum(length(s)) FROM file('${FILE}', Parquet, 's String') SETTINGS max_threads = 1, max_memory_usage = '1Gi'"
$CLICKHOUSE_CLIENT -q "${QUERY}, use_columns_cache = 1"
$CLICKHOUSE_CLIENT -q "${ENTRIES}"

echo "--- dictionary-encoded strings of 2 KiB without size statistics, written by PyArrow: read in small blocks, not cached"
# One row group of 65536 rows: a chunk takes 128 MiB decoded.
cp "${CUR_DIR}/data_parquet/dictionary_strings_without_size_statistics.parquet" "${FILE_PATH}"
touch -d "1 minute ago" "${FILE_PATH}"
$CLICKHOUSE_CLIENT -q "SELECT rg.columns.have_size_statistics FROM file('${FILE}', ParquetMetadata) ARRAY JOIN row_groups AS rg"
$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"
QUERY="SELECT count(), sum(length(s)) FROM file('${FILE}', Parquet, 's String')
    SETTINGS max_threads = 1, input_format_parquet_max_block_size = 1000, input_format_parquet_memory_high_watermark = '16Mi',
    max_memory_usage = '160Mi'"
$CLICKHOUSE_CLIENT -q "${QUERY}, use_columns_cache = 0"
$CLICKHOUSE_CLIENT -q "${QUERY}, use_columns_cache = 1"
$CLICKHOUSE_CLIENT -q "${ENTRIES}"

# One row group of one chunk, in pages of about 1000 rows with the page index. The strings are dictionary-encoded, so
# the file is small, but decoded, the column `s` takes about 32 MiB.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('${FILE}', Parquet)
    SELECT number AS id, leftPad(toString(number % 16), 512, 'x') AS s FROM numbers(65536)
    SETTINGS output_format_parquet_row_group_size = 1000000, output_format_parquet_data_page_size = 8192,
        output_format_parquet_batch_size = 1024, output_format_parquet_write_page_index = 1, engine_file_truncate_on_insert = 1"
touch -d "1 minute ago" "${FILE_PATH}"
$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(id), sum(length(s)) FROM file('${FILE}', Parquet, 'id UInt64, s String') SETTINGS max_threads = 1, use_columns_cache = 1"
$CLICKHOUSE_CLIENT -q "${ENTRIES}"

SETTINGS="max_threads = 1, input_format_parquet_max_block_size = 1000, input_format_parquet_memory_high_watermark = '4Mi',
    max_memory_usage = '24Mi'"

echo "--- the rows of a cached chunk left by the column index"
QUERY="SELECT count(), sum(length(s)) FROM file('${FILE}', Parquet, 'id UInt64, s String') WHERE id >= 4096
    SETTINGS ${SETTINGS}, optimize_move_to_prewhere = 0, use_top_k_dynamic_filtering = 0"
$CLICKHOUSE_CLIENT -q "${QUERY}, use_columns_cache = 0"
$CLICKHOUSE_CLIENT -q "${QUERY}, use_columns_cache = 1"

echo "--- a function of a cached chunk"
QUERY="SELECT sum(cityHash64(concat(s, '!'))) FROM file('${FILE}', Parquet, 's String') SETTINGS ${SETTINGS}"
$CLICKHOUSE_CLIENT -q "${QUERY}, use_columns_cache = 0"
$CLICKHOUSE_CLIENT -q "${QUERY}, use_columns_cache = 1"

rm -f "${FILE_PATH}"
