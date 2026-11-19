#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-parallel-replicas
# no-fasttest: Parquet support is not built in fast-test images
# no-parallel: the columns cache is system-wide, and the test drops it to reset its admission control
# no-parallel-replicas: profile events are not available on the second replica

# Lazy materialization reads the deferred columns of the requested rows only. With the columns cache, these rows are
# cached under their set: a repeated query takes them from the cache, and every query returns what it returns without
# the cache.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
FILE_PATH="${USER_FILES_PATH}/${FILE}"

# Four row groups (30000 rows each, the last one 10000), small pages, NULLs in `n` every 7 rows, an array column.
write_file()
{
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO FUNCTION file('${FILE}', Parquet)
        SELECT number AS id, number * 7919 % 100003 AS k, toString(number + $1) AS s, if(number % 7 = 0, NULL, number % 1000) AS n,
            toString(number % 23) AS d, repeat('x', number % 50) AS pad, [number, number + 1] AS arr
        FROM numbers(100000)
        SETTINGS output_format_parquet_row_group_size = 30000, output_format_parquet_data_page_size = 1024, engine_file_truncate_on_insert = 1"
    # Only a file that has not been modified for a few seconds is cached.
    touch -d "$2 minutes ago" "${FILE_PATH}"
}
write_file 0 2

SOURCE="file('${FILE}', Parquet)"
SOURCE_NOT_NULL="file('${FILE}', Parquet, 'id UInt64, k UInt64, s String, n UInt64, d String, pad String, arr Array(UInt64)')"
SETTINGS="enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 1, columns_cache_max_bytes_to_write_to_cache = 0, columns_cache_max_estimated_bytes_to_write_to_cache = 0, query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10000, max_threads = 4, optimize_move_to_prewhere = 1, use_query_condition_cache = 0"
HASH="count(), sum(cityHash64(id, k, s, ifNull(n, 0), d, pad, arraySum(arr)))"

# Other tests may have lowered the admission probability of the cache; dropping the cache resets it.
$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"

$CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM (EXPLAIN SELECT ${HASH} FROM (SELECT * FROM ${SOURCE} ORDER BY k, id LIMIT 5)) WHERE explain LIKE '%LazilyReadFromFile%' SETTINGS ${SETTINGS}"

# Once without the cache, then three times with it: the first read writes the rows, the next ones read them.
# Prints each distinct result with the number of runs that returned it.
check()
{
    for USE_CACHE in 0 1 1 1; do
        $CLICKHOUSE_CLIENT --log_comment "${CLICKHOUSE_TEST_UNIQUE_NAME}_$1_${USE_CACHE}" -q "$2 SETTINGS ${SETTINGS}, use_columns_cache = ${USE_CACHE}${3:+, $3}"
    done | sort | uniq -c | sed -e "s/^ *\([0-9]*\) /$1\t\1\t/"
}

check "top5" "SELECT ${HASH} FROM (SELECT * FROM ${SOURCE} ORDER BY k, id LIMIT 5)"
# Every column of the requested rows that can be cached has entries of these rows; the sort columns are read by the
# first pass, and the array is never cached this way.
$CLICKHOUSE_CLIENT -q "SELECT 'top5 columns', arraySort(groupUniqArray(column)) FROM system.columns_cache WHERE endsWith(part, '${FILE}') AND rows < row_end - row_begin AND column NOT IN ('id', 'k')"
check "top4" "SELECT ${HASH} FROM (SELECT * FROM ${SOURCE} ORDER BY k, id LIMIT 4)"
check "top3000" "SELECT ${HASH} FROM (SELECT * FROM ${SOURCE} ORDER BY k, id LIMIT 3000)"
check "filtered" "SELECT ${HASH} FROM (SELECT * FROM ${SOURCE} WHERE id % 1000 = 17 ORDER BY k DESC, id LIMIT 3 OFFSET 2)"
check "rows" "SELECT * FROM ${SOURCE} ORDER BY k DESC, id LIMIT 3"
# NULLs replaced with defaults in the requested rows: such a column is not cached.
check "null as default" "SELECT count(), sum(n), countIf(n = 0) FROM (SELECT * FROM ${SOURCE_NOT_NULL} ORDER BY k, id LIMIT 300)" "input_format_null_as_default = 1"
# The same with the `DEFAULT` expression of `n`, which a cached column could not fill in.
SOURCE_DEFAULT="file('${FILE}', Parquet, 'id UInt64, k UInt64, s String, n UInt64 DEFAULT 42, d String, pad String, arr Array(UInt64)')"
$CLICKHOUSE_CLIENT -q "SELECT 'default expression lazy', count() > 0 FROM (EXPLAIN SELECT count(), sum(n) FROM (SELECT * FROM ${SOURCE_DEFAULT} ORDER BY k, id LIMIT 300)) WHERE explain LIKE '%LazilyReadFromFile%' SETTINGS ${SETTINGS}"
check "default expression" "SELECT count(), sum(n), countIf(n = 42) FROM (SELECT * FROM ${SOURCE_DEFAULT} ORDER BY k, id LIMIT 300)" "input_format_null_as_default = 1"

# A rewritten file is a new file for the cache: the same rows of it are not taken from the entries of the old one.
write_file 1 1
check "rewritten" "SELECT ${HASH} FROM (SELECT * FROM ${SOURCE} ORDER BY k, id LIMIT 5)"
check "rewritten rows" "SELECT * FROM ${SOURCE} ORDER BY k DESC, id LIMIT 3"
# The read of the requested rows charges the estimate budget of the query with their share of each chunk: under a budget
# of one byte it writes nothing, although the sort columns, read first, come from the cache and charge nothing.
$CLICKHOUSE_CLIENT -q "SELECT sum(k), sum(id) FROM ${SOURCE} SETTINGS ${SETTINGS}, use_columns_cache = 1 FORMAT Null"
ESTIMATE="${SETTINGS/columns_cache_max_estimated_bytes_to_write_to_cache = 0/columns_cache_max_estimated_bytes_to_write_to_cache = 1}, use_top_k_dynamic_filtering = 0"
for USE_CACHE in 0 1 1; do
    $CLICKHOUSE_CLIENT --log_comment "${CLICKHOUSE_TEST_UNIQUE_NAME}_estimate_${USE_CACHE}" -q "SELECT ${HASH} FROM (SELECT * FROM ${SOURCE} ORDER BY k, id LIMIT 5 OFFSET 20) SETTINGS ${ESTIMATE}, use_columns_cache = ${USE_CACHE}"
done | sort | uniq -c | sed -e "s/^ *\([0-9]*\) /estimate\t\1\t/"
$CLICKHOUSE_CLIENT -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT 'estimate events', sum(ProfileEvents['ParquetFilteredColumnsCacheHits']) = 0, sum(ProfileEvents['ParquetFilteredColumnsCacheMisses']) > 0
    FROM system.query_log
    WHERE log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_estimate_1' AND type = 'QueryFinish' AND current_database = currentDatabase()"
# A constant column takes a few bytes per row group, so the share of it the requested rows take is below one byte. It is
# still charged, rounded up, so under the same budget this read writes nothing either.
CONST="${CLICKHOUSE_TEST_UNIQUE_NAME}_const.parquet"
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('${CONST}', Parquet)
    SELECT number AS id, number * 7919 % 100003 AS k, 'c' AS c FROM numbers(100000)
    SETTINGS output_format_parquet_row_group_size = 30000, engine_file_truncate_on_insert = 1"
touch -d "2 minutes ago" "${USER_FILES_PATH}/${CONST}"
$CLICKHOUSE_CLIENT -q "SELECT sum(k), sum(id) FROM file('${CONST}', Parquet) SETTINGS ${SETTINGS}, use_columns_cache = 1 FORMAT Null"
for USE_CACHE in 0 1 1; do
    $CLICKHOUSE_CLIENT --log_comment "${CLICKHOUSE_TEST_UNIQUE_NAME}_estimate_const_${USE_CACHE}" -q "SELECT count(), sum(cityHash64(id, k, c)) FROM (SELECT * FROM file('${CONST}', Parquet) ORDER BY k, id LIMIT 5) SETTINGS ${ESTIMATE}, use_columns_cache = ${USE_CACHE}"
done | sort | uniq -c | sed -e "s/^ *\([0-9]*\) /estimate const\t\1\t/"
$CLICKHOUSE_CLIENT -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT 'estimate const events', sum(ProfileEvents['ParquetFilteredColumnsCacheHits']) = 0, sum(ProfileEvents['ParquetFilteredColumnsCacheMisses']) > 0
    FROM system.query_log
    WHERE log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_estimate_const_1' AND type = 'QueryFinish' AND current_database = currentDatabase()"

echo "--- the read of the requested rows used the cache"
$CLICKHOUSE_CLIENT -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT sum(ProfileEvents['ParquetFilteredColumnsCacheHits']) > 0, sum(ProfileEvents['ParquetFilteredColumnsCacheMisses']) > 0
    FROM system.query_log
    WHERE log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_top5_1' AND type = 'QueryFinish' AND current_database = currentDatabase()"

rm -f "${FILE_PATH}"
rm -f "${USER_FILES_PATH}/${CONST}"
