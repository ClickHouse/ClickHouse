#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-parallel-replicas
# no-fasttest: Parquet support is not built in fast-test images
# no-parallel: the columns cache is system-wide, and the test drops it to reset its admission control
# no-parallel-replicas: profile events are not available on the second replica

# A column served from the columns cache after a selective `PREWHERE` on another column: only the rows
# that passed are copied from the cached column, so the read needs no more memory than without the cache.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
FILE_PATH="${USER_FILES_PATH}/${FILE}"

# One row group with a payload column of about 32 MiB.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('${FILE}', Parquet)
    SELECT number AS id, leftPad(toString(number), 2048, 'x') AS s FROM numbers(16384)
    SETTINGS output_format_parquet_row_group_size = 16384, engine_file_truncate_on_insert = 1"
# Only a file that has not been modified for a few seconds is cached.
touch -d "1 minute ago" "${FILE_PATH}"

SOURCE="file('${FILE}', Parquet, 'id UInt64, s String')"
CACHED="SELECT count() FROM system.columns_cache WHERE database = '' AND endsWith(part, '/${FILE}') AND column = 's'"

# Other tests may have lowered the admission probability of the cache; dropping the cache resets it.
$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"

# The admission control of the cache can skip a write: read the file until the payload column is there.
# Without `optimize_functions_to_subcolumns = 0`, `length(s)` reads only the sizes of `s`, not `s` itself.
for _ in {1..20}; do
    $CLICKHOUSE_CLIENT -q "SELECT sum(id), sum(length(s)) FROM ${SOURCE} FORMAT Null SETTINGS
        use_columns_cache = 1, enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 1,
        columns_cache_max_bytes_to_write_to_cache = 0, columns_cache_max_estimated_bytes_to_write_to_cache = 0,
        optimize_functions_to_subcolumns = 0"
    [ "$($CLICKHOUSE_CLIENT -q "${CACHED}")" = "1" ] && break
done
$CLICKHOUSE_CLIENT -q "${CACHED}"

READ_SETTINGS="max_threads = 1, max_parsing_threads = 1, enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 0"

for PREDICATE in "id = 1" "id % 7 = 3"; do
    for BLOCK_SIZE in 16384 1000; do
        echo "--- ${PREDICATE}, block size ${BLOCK_SIZE}"
        for USE_CACHE in 0 1; do
            $CLICKHOUSE_CLIENT --log_comment "${CLICKHOUSE_TEST_UNIQUE_NAME}_${USE_CACHE}" -q "
                SELECT count(), sum(id), sum(length(s)), sum(toUInt64(replaceAll(s, 'x', '')) != id) FROM ${SOURCE}
                PREWHERE ${PREDICATE}
                SETTINGS ${READ_SETTINGS}, input_format_parquet_max_block_size = ${BLOCK_SIZE},
                    use_columns_cache = ${USE_CACHE}, max_memory_usage = '16Mi'"
        done
    done
done

echo "--- the cache was used"
$CLICKHOUSE_CLIENT -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT count(), countIf(ProfileEvents['ColumnsCacheHits'] > 0) FROM system.query_log
    WHERE log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_1' AND type = 'QueryFinish' AND current_database = currentDatabase()"

rm -f "${FILE_PATH}"
