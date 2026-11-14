#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# Deleted rows must not hide a data file without metrics. The statistics of a column count the rows of the files that
# have its metrics, deleted rows included, and cover only part of the table when they count fewer rows than the files
# hold. Compared with the rows left after the deletes instead, a delete of as many rows as the file without metrics
# has made the bounds of the other files look enclosing, so they survived `GROUP BY` for DPsub's range costing.
# Fixture of `05339_iceberg_manifest_stats_null_counts` (Spark 3.5.5 / Iceberg 1.10): 2 data files of 1000 rows, `k`
# 0..999 in the first and 1000..1999 in the second, `x` NULL in every other row and without metrics in the second
# file. The DELETE removes every row of the first file, the only one with bounds of `x`, by a position delete file
# scoped to it, so every live value of `x` lies outside those bounds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT_TRACE=${CLICKHOUSE_CLIENT/"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"/"--send_logs_level=trace"}

# The trace line is printed by join reordering, which the randomized settings can turn off; the delete files depend on
# the insert threads and block size.
PINS="--query_plan_optimize_join_order_randomize=0 --query_plan_optimize_join_order_limit=10
    --use_hash_table_stats_for_join_reordering=0 --enable_join_runtime_filters=0 --enable_parallel_replicas=0
    --use_iceberg_manifest_statistics=1 --use_statistics=1 --allow_insert_into_iceberg=1
    --max_insert_threads=1 --max_threads=1 --max_block_size=1000000"

LAKE="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${LAKE}"
mkdir -p "${LAKE}"
cp -r "${CUR_DIR}/data_minio/null_counts_db/metrics_none_table" "${LAKE}/metrics_none_table"

${CLICKHOUSE_CLIENT} ${PINS} --query "
    CREATE TABLE metrics_none ENGINE = IcebergLocal('${LAKE}/metrics_none_table');
    CREATE TABLE dim10 (k Int32) ENGINE = MergeTree ORDER BY k;
    INSERT INTO dim10 SELECT number FROM numbers(10);
    DELETE FROM metrics_none WHERE k < 1000;"

echo '--- fixture after the DELETE: content, rows and whether the file has a NULL count of x (field id 2)'
${CLICKHOUSE_CLIENT} --query "
    SELECT content, record_count, mapContains(null_value_counts, 2) FROM system.iceberg_files
    WHERE database = currentDatabase() AND table = 'metrics_none'
    ORDER BY content, mapContains(null_value_counts, 2) DESC"
echo "live rows, min and max of x: $(${CLICKHOUSE_CLIENT} --query "SELECT count(), min(x), max(x) FROM metrics_none" | tr '\t' ' ')"

# Prints the provenance of column `$2` in the `Estimated statistics` trace line of relation `$1` for query `$3`.
provenance()
{
    ${CLICKHOUSE_CLIENT_TRACE} ${PINS} --query "$3" 2>&1 >/dev/null \
        | grep -oE "Estimated statistics for [A-Za-z]+ $1: .*" | head -n 1 \
        | grep -oE "__table1\.$2: [^(]*\([^)]*\)" | sed -E 's/: [^(]*\(/: (/'
}

echo '--- the read: 1000 live rows, as many as the statistics of x count, which still miss the second file'
READ="EXPLAIN SELECT metrics_none.x FROM metrics_none JOIN dim10 ON metrics_none.k = dim10.k"
provenance metrics_none x "${READ}"
echo '--- control: k has metrics in both files, so its range only drops rows'
provenance metrics_none k "${READ}"
echo '--- behind GROUP BY, which keeps only enclosing ranges'
provenance g x "EXPLAIN SELECT g.x FROM (SELECT x, any(k) AS k FROM metrics_none GROUP BY x) AS g JOIN dim10 ON g.k = dim10.k"

rm -rf "${LAKE}"
