#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# Issue 120440: an identity partition column without metrics gets its NULL count and bounds from the partition value of
# each file, so it keeps its statistics and the number of distinct partition values (rule 1).
# Fixture written by Spark 3.5.5 / Iceberg 1.10: `PARTITIONED BY (r)` with `write.metadata.metrics.column.r = none`,
# 1200 rows in two inserts, `r` one of 'us', 'eu', 'asia' and NULL.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT_TRACE=${CLICKHOUSE_CLIENT/"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"/"--send_logs_level=trace"}

# The trace line is printed by join reordering, which the randomized settings can turn off.
PINS="--query_plan_optimize_join_order_randomize=0 --query_plan_optimize_join_order_limit=10
    --use_hash_table_stats_for_join_reordering=0 --enable_join_runtime_filters=0 --enable_parallel_replicas=0
    --use_iceberg_manifest_statistics=1 --use_statistics=1"

LAKE="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${LAKE}"
mkdir -p "${LAKE}"
cp -r "${CUR_DIR}/data_minio/identity_partition_db/metrics_none_table" "${LAKE}/metrics_none_table"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ident ENGINE = IcebergLocal('${LAKE}/metrics_none_table');
    CREATE TABLE dim10 (k Int32) ENGINE = MergeTree ORDER BY k;
    INSERT INTO dim10 SELECT number FROM numbers(10);"

echo '--- fixture: data files, rows, files with a NULL count of r (field id 2), partitions'
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), sum(record_count), countIf(mapContains(null_value_counts, 2)), uniqExact(partition)
    FROM system.iceberg_files WHERE database = currentDatabase() AND table = 'ident'"
echo "distinct r, NULL fraction: $(${CLICKHOUSE_CLIENT} --query "SELECT uniqExact(r), countIf(r IS NULL) / count() FROM ident")"

echo '--- the Estimated statistics trace line of r'
echo 'Expect 3 [null 0.25]: the non-NULL partition values and the rows of the NULL partition; without them r had no entry.'
${CLICKHOUSE_CLIENT_TRACE} ${PINS} --query "
    EXPLAIN SELECT ident.r FROM ident JOIN dim10 ON ident.k = dim10.k" 2>&1 >/dev/null \
    | grep -oE "Estimated statistics for [A-Za-z]+ ident: .*" | head -n 1 | grep -oE '__table1\.r: [0-9]+( \[[^]]*\])?'

rm -rf "${LAKE}"
