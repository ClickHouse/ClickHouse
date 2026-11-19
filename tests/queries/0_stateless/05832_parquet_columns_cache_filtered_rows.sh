#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-parallel-replicas
# no-fasttest: Parquet support is not built in fast-test images
# no-parallel: the columns cache is system-wide, and the test drops it to reset its admission control
# no-parallel-replicas: profile events are not available on the second replica

# A Parquet column read after a `PREWHERE` step is cached under the set of rows that passed the earlier steps: a later
# read of the same rows takes it from the cache, and every query returns what it returns without the cache.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
FILE_PATH="${USER_FILES_PATH}/${FILE}"

# Two row groups of 200000 and 100000 rows, NULLs in `n` every 1000 rows.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('${FILE}', Parquet)
    SELECT number AS k, number % 7 AS f, toString(number * 7919) AS s, if(number % 1000 = 999, NULL, number) AS n, toInt16(number % 300) AS w
    FROM numbers(300000)
    SETTINGS output_format_parquet_row_group_size = 200000, engine_file_truncate_on_insert = 1"
# Only a file that has not been modified for a few seconds is cached.
touch -d "1 minute ago" "${FILE_PATH}"

SOURCE="file('${FILE}', Parquet, 'k UInt64, f UInt64, s String, n Nullable(UInt64), w Int16')"
SOURCE_NOT_NULL="file('${FILE}', Parquet, 'k UInt64, f UInt64, s String, n UInt64, w Int16')"
SETTINGS="enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 1, columns_cache_max_bytes_to_write_to_cache = 0, columns_cache_max_estimated_bytes_to_write_to_cache = 0, optimize_move_to_prewhere = 1, use_query_condition_cache = 0"

# Other tests may have lowered the admission probability of the cache; dropping the cache resets it.
$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"

# Once without the cache, then three times with it: the first read writes the columns, the next ones read them.
# Prints each distinct result with the number of runs that returned it.
check()
{
    for USE_CACHE in 0 1 1 1; do
        $CLICKHOUSE_CLIENT --log_comment "${CLICKHOUSE_TEST_UNIQUE_NAME}_${USE_CACHE}" -q "$2 SETTINGS ${SETTINGS}, use_columns_cache = ${USE_CACHE}${3:+, $3}"
    done | sort | uniq -c | sed -e "s/^ *\([0-9]*\) /$1\t\1\t/"
}

check "f = 3" "SELECT count(), sum(cityHash64(s)), sum(w) FROM ${SOURCE} WHERE f = 3"
check "f = 5" "SELECT count(), sum(cityHash64(s)), sum(w) FROM ${SOURCE} WHERE f = 5"
check "two steps" "SELECT count(), sum(cityHash64(s)), sum(w) FROM ${SOURCE} WHERE f = 3 AND w > 100"
# The same rows selected by two different conditions.
check "same rows 1" "SELECT count(), sum(cityHash64(s)), sum(w) FROM ${SOURCE} WHERE f = 3 AND k % 2 = 0"
check "same rows 2" "SELECT count(), sum(cityHash64(s)), sum(w) FROM ${SOURCE} WHERE k % 7 = 3 AND k % 2 = 0"
# The same rows under a third condition, on `k` alone and read-only: only `s` and `w` are looked up, and only the
# entries the two reads above wrote under other conditions can serve them.
$CLICKHOUSE_CLIENT --log_comment "${CLICKHOUSE_TEST_UNIQUE_NAME}_same_rows_3" -q "SELECT count(), sum(cityHash64(s)), sum(w) FROM ${SOURCE} WHERE k % 14 = 10 SETTINGS ${SETTINGS/enable_writes_to_columns_cache = 1/enable_writes_to_columns_cache = 0}, use_columns_cache = 1" | sed -e "s/^/same rows 3\t/"
$CLICKHOUSE_CLIENT -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT 'same rows 3 hits', sum(ProfileEvents['ParquetFilteredColumnsCacheHits']) > 0
    FROM system.query_log
    WHERE log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_same_rows_3' AND type = 'QueryFinish' AND current_database = currentDatabase()"
# A nondeterministic filter: the values of the columns read after it belong to the rows of `k` read before it.
check "rand" "SELECT countIf(w != toInt16(k % 300)), countIf(s != toString(k * 7919)), count() > 0 FROM ${SOURCE} WHERE f = 3 AND k % 2 = 0 AND rand() % 2 = 0"
# NULLs replaced with the `DEFAULT` expression of `n`: the read reports them as missing values for the expression to fill
# in, which the cache does not keep, so `n` is not cached and every run returns the `DEFAULT` values.
check "default expression" "SELECT count(), sum(n), countIf(n = 42) FROM file('${FILE}', Parquet, 'f UInt64, n UInt64 DEFAULT 42') WHERE f = 3" "input_format_null_as_default = 1"
# NULLs replaced with defaults in the rows that pass, and NULLs only in the rows that do not.
check "null as default" "SELECT count(), sum(n), countIf(n = 0) FROM ${SOURCE_NOT_NULL} WHERE f = 3" "input_format_null_as_default = 1"
check "nulls rejected" "SELECT count(), sum(n) FROM ${SOURCE_NOT_NULL} WHERE f = 3 AND k % 1000 != 999" "input_format_null_as_default = 0"
check "nullable" "SELECT count(), sum(n), countIf(n IS NULL) FROM ${SOURCE} WHERE f = 4"

echo "--- the cache was used"
$CLICKHOUSE_CLIENT -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT sum(ProfileEvents['ParquetFilteredColumnsCacheHits']) > 0, sum(ProfileEvents['ParquetFilteredColumnsCacheMisses']) > 0
    FROM system.query_log
    WHERE log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_1' AND type = 'QueryFinish' AND current_database = currentDatabase()"

echo "--- an entry of the rows that passed holds fewer rows than its range"
$CLICKHOUSE_CLIENT -q "
    SELECT countIf(rows > row_end - row_begin) = 0, countIf(rows < row_end - row_begin) > 0
    FROM system.columns_cache WHERE endsWith(part, '${FILE}')"

rm -f "${FILE_PATH}"
