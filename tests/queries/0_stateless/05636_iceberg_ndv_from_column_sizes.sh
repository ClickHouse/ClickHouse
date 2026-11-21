#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# With `estimate_distinct_values_from_metadata`, an Iceberg read estimates the number of distinct values of a column
# that no other rule gives from the `column_sizes` of its data files in the manifests: 2 to the power of the compressed
# bits per row. The fixture is the one of 05507_iceberg_table_statistics_ndv, 3000 rows in one file written by Spark,
# whose columns `s`, `dec`, `d` and `n` repeat with periods of 50, 6, 11 and 7 rows, which compress to about one bit
# per row; `sid` is unique and does not compress, so it keeps the 10% of the rows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT_TRACE=${CLICKHOUSE_CLIENT/"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"/"--send_logs_level=trace"}

PINS="--query_plan_optimize_join_order_randomize=0 --query_plan_optimize_join_order_limit=10
    --query_plan_optimize_join_order_algorithm=greedy --query_plan_join_swap_table=auto
    --use_hash_table_stats_for_join_reordering=0 --collect_hash_table_stats_during_joins=0
    --enable_join_runtime_filters=0 --enable_parallel_replicas=0 --use_statistics=1
    --use_datalake_metadata_statistics=1 --use_iceberg_table_statistics_ndv=0 --explain_query_plan_default=legacy"

LAKE="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${LAKE}"
mkdir -p "${LAKE}"
cp -r "${CUR_DIR}/data_minio/table_statistics_db/ndv_table" "${LAKE}/ndv_table"

# Prints the numbers of distinct values of the `Estimated statistics` trace line. Usage: ndv [client flags].
ndv()
{
    ${CLICKHOUSE_CLIENT_TRACE} ${PINS} "$@" --query "
        EXPLAIN SELECT t.k, t.s, t.dec, t.d, t.sid, t.n FROM t JOIN dim ON t.k = dim.k" 2>&1 >/dev/null \
        | grep -oE "Estimated statistics for [A-Za-z]+ t: .*" | head -n 1 \
        | grep -oE '__table1\.[a-z]+: [0-9]+' | sed 's/__table1\.//' | sort
}

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE t ENGINE = IcebergLocal('${LAKE}/ndv_table');
    CREATE TABLE dim (k Int64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'uniq';
    INSERT INTO dim SELECT number FROM numbers(10);"

echo '--- From the sizes of the columns'
ndv --estimate_distinct_values_from_metadata=1

echo '--- Without: 10% of the rows, except k, from the range of its bounds'
ndv --estimate_distinct_values_from_metadata=0

${CLICKHOUSE_CLIENT} --query "DROP TABLE t; DROP TABLE dim;"
rm -rf "${LAKE}"
