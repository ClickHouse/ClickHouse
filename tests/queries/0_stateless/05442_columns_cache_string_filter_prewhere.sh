#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-parallel-replicas, no-random-settings, no-random-merge-tree-settings
# no-fasttest: Parquet support is not built in fast-test images
# no-parallel: the columns cache is system-wide, and the test drops it and checks what it holds
# no-parallel-replicas: profile events are not available on the second replica
# no-random-settings, no-random-merge-tree-settings: the test checks the PREWHERE decision, the scan-time
# string filter (which is not applied to the sparse serialization) and what is written to the columns cache

# With the columns cache enabled and writes to it enabled, a substring search condition on the only queried
# column is still moved to PREWHERE (`apply_string_filters_during_scan`). The reader does not apply the
# scan-time string filter to the columns it writes to the cache: the cache must never keep the empty
# strings substituted for the values that do not match. Checked for `MergeTree` and for `Parquet`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, apply_string_filters_during_scan = 1, enable_parallel_replicas = 0"
CACHE="use_columns_cache = 1, enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 1"
CACHE_LIMITS="columns_cache_max_bytes_to_write_to_cache = 0, columns_cache_max_estimated_bytes_to_write_to_cache = 0"

function events()
{
    # The cache and string filter events of the query with the log comment $1.
    $CLICKHOUSE_CLIENT -q "
        SYSTEM FLUSH LOGS query_log;
        SELECT '$1', ProfileEvents['ColumnsCacheHits'] > 0, ProfileEvents['StringValueFilterValuesChecked'] > 0
        FROM system.query_log
        WHERE log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_$1' AND type = 'QueryFinish' AND current_database = currentDatabase()
        ORDER BY event_time_microseconds"
}

echo "--- MergeTree"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_wide;
    DROP TABLE IF EXISTS t_compact;
    CREATE TABLE t_wide (id UInt32, s String) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1.0;
    CREATE TABLE t_compact (id UInt32, s String) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 1000000000, ratio_of_defaults_for_sparse_serialization = 1.0;
    INSERT INTO t_wide SELECT number, if(number % 100 = 0, 'needle ' || toString(number), 'haystack ' || toString(number)) FROM numbers(10000);
    INSERT INTO t_compact SELECT * FROM t_wide;
"

echo "PREWHERE"
for USE_CACHE in "use_columns_cache = 0" "${CACHE}" "${CACHE}, enable_writes_to_columns_cache = 0"; do
    $CLICKHOUSE_CLIENT -q "
        SELECT countIf(explain LIKE '%Prewhere filter column%LIKE%') > 0 FROM (
            EXPLAIN actions = 1 SELECT count() FROM t_wide WHERE s LIKE '%needle%' SETTINGS ${SETTINGS}, ${USE_CACHE})"
done

$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"

# The filtered query writes the column to the cache: the reader does not apply the scan-time filter to it.
$CLICKHOUSE_CLIENT --log_comment "${CLICKHOUSE_TEST_UNIQUE_NAME}_wide_filtered" -q "
    SELECT count(), sum(length(s)) FROM t_wide WHERE s LIKE '%needle%' SETTINGS ${SETTINGS}, ${CACHE}, ${CACHE_LIMITS}"
events wide_filtered
$CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_wide' AND column = 's'"

# The unfiltered query reads the column from the cache: the values must be the same as without the cache.
for USE_CACHE in "use_columns_cache = 0" "${CACHE}"; do
    $CLICKHOUSE_CLIENT --log_comment "${CLICKHOUSE_TEST_UNIQUE_NAME}_wide_full" -q "
        SELECT count(), countIf(empty(s)), sum(length(s)), groupBitXor(cityHash64(s)) FROM t_wide SETTINGS ${SETTINGS}, ${USE_CACHE}"
done
events wide_full

# The filtered query served from the cache.
$CLICKHOUSE_CLIENT --log_comment "${CLICKHOUSE_TEST_UNIQUE_NAME}_wide_cached" -q "
    SELECT count(), sum(length(s)) FROM t_wide WHERE s LIKE '%needle%' SETTINGS ${SETTINGS}, ${CACHE}"
events wide_cached

# Without writes to the cache, the filter is applied.
$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"
$CLICKHOUSE_CLIENT --log_comment "${CLICKHOUSE_TEST_UNIQUE_NAME}_wide_no_writes" -q "
    SELECT count(), sum(length(s)) FROM t_wide WHERE s LIKE '%needle%' SETTINGS ${SETTINGS}, ${CACHE}, enable_writes_to_columns_cache = 0"
events wide_no_writes

# A compact part is written to the cache as well: the reader does not apply the scan-time filter to it.
$CLICKHOUSE_CLIENT --log_comment "${CLICKHOUSE_TEST_UNIQUE_NAME}_compact_filtered" -q "
    SELECT count(), sum(length(s)) FROM t_compact WHERE s LIKE '%needle%' SETTINGS ${SETTINGS}, ${CACHE}, ${CACHE_LIMITS}"
events compact_filtered
$CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_compact' AND column = 's'"
for USE_CACHE in "use_columns_cache = 0" "${CACHE}"; do
    $CLICKHOUSE_CLIENT --log_comment "${CLICKHOUSE_TEST_UNIQUE_NAME}_compact_full" -q "
        SELECT count(), countIf(empty(s)), sum(length(s)), groupBitXor(cityHash64(s)) FROM t_compact SETTINGS ${SETTINGS}, ${USE_CACHE}"
done
events compact_full

$CLICKHOUSE_CLIENT -q "
    DROP TABLE t_wide;
    DROP TABLE t_compact;
"

echo "--- Parquet"

FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
FILE_PATH="${USER_FILES_PATH}/${FILE}"
SOURCE="file('${FILE}', Parquet, 'id UInt64, s String')"

$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('${FILE}', Parquet, 'id UInt64, s String')
    SELECT number, if(number % 100 = 0, 'needle ' || toString(number), 'haystack ' || toString(number)) FROM numbers(10000)
    SETTINGS output_format_parquet_row_group_size = 1000, output_format_parquet_max_dictionary_size = 0, engine_file_truncate_on_insert = 1"
# Only a file that has not been modified for a few seconds is cached.
touch -d "1 minute ago" "${FILE_PATH}"

echo "PREWHERE"
for USE_CACHE in "use_columns_cache = 0" "${CACHE}" "${CACHE}, enable_writes_to_columns_cache = 0"; do
    $CLICKHOUSE_CLIENT -q "
        SELECT countIf(explain LIKE '%Prewhere filter column%LIKE%') > 0 FROM (
            EXPLAIN actions = 1 SELECT count() FROM ${SOURCE} WHERE s LIKE '%needle%' SETTINGS ${SETTINGS}, ${USE_CACHE})"
done

$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"

# The filtered query writes the column to the cache: the reader does not apply the scan-time filter to it.
$CLICKHOUSE_CLIENT --log_comment "${CLICKHOUSE_TEST_UNIQUE_NAME}_parquet_filtered" -q "
    SELECT count(), sum(length(s)) FROM ${SOURCE} WHERE s LIKE '%needle%' SETTINGS ${SETTINGS}, ${CACHE}, ${CACHE_LIMITS}"
events parquet_filtered
$CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM system.columns_cache WHERE database = '' AND endsWith(part, '/${FILE}') AND column = 's'"

# The unfiltered query reads the column from the cache: the values must be the same as without the cache.
for USE_CACHE in "use_columns_cache = 0" "${CACHE}"; do
    $CLICKHOUSE_CLIENT --log_comment "${CLICKHOUSE_TEST_UNIQUE_NAME}_parquet_full" -q "
        SELECT count(), countIf(empty(s)), sum(length(s)), groupBitXor(cityHash64(s)) FROM ${SOURCE} SETTINGS ${SETTINGS}, ${USE_CACHE}"
done
events parquet_full

# The filtered query served from the cache.
$CLICKHOUSE_CLIENT --log_comment "${CLICKHOUSE_TEST_UNIQUE_NAME}_parquet_cached" -q "
    SELECT count(), sum(length(s)) FROM ${SOURCE} WHERE s LIKE '%needle%' SETTINGS ${SETTINGS}, ${CACHE}"
events parquet_cached

# Without writes to the cache, the filter is applied.
$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"
$CLICKHOUSE_CLIENT --log_comment "${CLICKHOUSE_TEST_UNIQUE_NAME}_parquet_no_writes" -q "
    SELECT count(), sum(length(s)) FROM ${SOURCE} WHERE s LIKE '%needle%' SETTINGS ${SETTINGS}, ${CACHE}, enable_writes_to_columns_cache = 0"
events parquet_no_writes

rm -f "${FILE_PATH}"
