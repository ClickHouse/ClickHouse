#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# Issue 120440: the manifest statistics of a `String` column merge over every data file, as those of a number do. They are
# built without string lengths, and the estimator merges them into a copy made from the column type, which tracks string
# lengths; the copy and the files' statistics must still count as compatible, or every file after the first is skipped.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT_TRACE=${CLICKHOUSE_CLIENT/"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"/"--send_logs_level=trace"}

# The trace line is printed by join reordering, which the randomized settings can turn off.
PINS="--query_plan_optimize_join_order_randomize=0 --query_plan_optimize_join_order_limit=10
    --use_hash_table_stats_for_join_reordering=0 --enable_join_runtime_filters=0 --enable_parallel_replicas=0
    --use_iceberg_manifest_statistics=1 --use_statistics=1 --max_insert_threads=1 --allow_insert_into_iceberg=1"

LAKE="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${LAKE}"
mkdir -p "${LAKE}"

# Two data files of 1000 rows: `s` is NULL in every row of the first and in no row of the second.
${CLICKHOUSE_CLIENT} ${PINS} --query "
    CREATE TABLE strings (k Int32, s Nullable(String)) ENGINE = IcebergLocal('${LAKE}/strings');
    INSERT INTO strings SELECT number, NULL FROM numbers(1000);
    INSERT INTO strings SELECT number, toString(number) FROM numbers(1000);
    CREATE TABLE dim10 (k Int32) ENGINE = MergeTree ORDER BY k;
    INSERT INTO dim10 SELECT number FROM numbers(10);"

echo '--- fixture: rows and the NULL count of s (field id 2) per data file'
${CLICKHOUSE_CLIENT} --query "
    SELECT record_count, null_value_counts[2] FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = 'strings' ORDER BY null_value_counts[2] DESC"

echo '--- the Estimated statistics trace line of s'
echo 'Expect null 0.5: 1000 NULLs in 2000 rows; with the first file alone it was 1.'
${CLICKHOUSE_CLIENT_TRACE} ${PINS} --query "
    EXPLAIN SELECT strings.s FROM strings JOIN dim10 ON strings.k = dim10.k" 2>&1 >/dev/null \
    | grep -oE "Estimated statistics for [A-Za-z]+ strings: .*" | head -n 1 | grep -oE '__table1\.s: [0-9]+ \[[^]]*\]'

rm -rf "${LAKE}"
