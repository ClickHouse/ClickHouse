#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: Parquet support is not built in fast-test images
# no-parallel: the columns cache is system-wide, and the test drops it to reset its admission control

# The rows of a column served from the columns cache are copied to the output of the `Parquet` reader,
# and the reader reserves memory for the copies. For dictionary-encoded strings without the optional
# size statistics, the estimate from the metadata relies on the dictionary, which is not read for a
# cached column, so the reservation has to be based on the cached column itself. Otherwise a `PREWHERE`
# read queues the copies of many row groups and needs much more memory than without the cache.
#
# The file has 81920 strings of 1024 bytes in 79 dictionary-encoded row groups, without
# `ColumnMetaData.size_statistics`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
FILE_PATH="${USER_FILES_PATH}/${FILE}"

cp "$CUR_DIR/data_parquet/dictionary_strings_without_size_statistics.parquet" "${FILE_PATH}"
# Only a file that has not been modified for a few seconds is cached.
touch -d "1 minute ago" "${FILE_PATH}"

QUERY="SELECT sum(sipHash64(s)) FROM file('${FILE}', Parquet, 's String') PREWHERE endsWith(s, '0')"
LIMITS="max_threads = 1, max_parsing_threads = 4, input_format_parquet_max_block_size = 256,
    input_format_parquet_memory_low_watermark = '1Mi', input_format_parquet_memory_high_watermark = '2Mi',
    input_format_parquet_dictionary_filter_push_down = 0, apply_string_filters_during_scan = 0"

ROW_GROUPS=$($CLICKHOUSE_CLIENT -q "SELECT num_row_groups FROM file('${FILE}', ParquetMetadata)")

# Other tests may have lowered the admission probability of the cache; dropping the cache resets it.
$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"

# The admission control of the cache can skip a write: read the file until all its row groups are there.
for _ in {1..20}; do
    $CLICKHOUSE_CLIENT -q "${QUERY} FORMAT Null SETTINGS ${LIMITS},
        use_columns_cache = 1, enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 1,
        columns_cache_max_bytes_to_write_to_cache = 0, columns_cache_max_estimated_bytes_to_write_to_cache = 0"
    [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.columns_cache WHERE database = '' AND endsWith(part, '/${FILE}')")" = "${ROW_GROUPS}" ] && break
done

echo "--- without the cache"
$CLICKHOUSE_CLIENT -q "${QUERY} SETTINGS ${LIMITS}, max_memory_usage = '20Mi', use_columns_cache = 0"
echo "--- from the cache"
$CLICKHOUSE_CLIENT -q "${QUERY} SETTINGS ${LIMITS}, max_memory_usage = '20Mi',
    use_columns_cache = 1, enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 0"
$CLICKHOUSE_CLIENT -q "SELECT count() = ${ROW_GROUPS} FROM system.columns_cache WHERE database = '' AND endsWith(part, '/${FILE}')"

rm -f "${FILE_PATH}"
