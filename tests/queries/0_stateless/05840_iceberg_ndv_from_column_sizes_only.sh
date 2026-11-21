#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# The NULL counts, the bounds and the sizes of the columns of a data file are optional in an Iceberg manifest,
# independently of each other. With `estimate_distinct_values_from_metadata`, a column whose files have only the sizes
# still gets its number of distinct values from them. The fixture is the one of 05636_iceberg_ndv_from_column_sizes
# with a manifest without `null_value_counts`, `lower_bounds` and `upper_bounds` (rewritten with `fastavro`, keeping
# its schema and metadata). The manifest keeps the path of the original one inside the table, so the cache of the
# metadata files is not used: it could give the manifest of the original fixture, read by another test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT_TRACE=${CLICKHOUSE_CLIENT/"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"/"--send_logs_level=trace"}

PINS="--query_plan_optimize_join_order_randomize=0 --query_plan_optimize_join_order_limit=10
    --query_plan_optimize_join_order_algorithm=greedy --query_plan_join_swap_table=auto
    --use_hash_table_stats_for_join_reordering=0 --collect_hash_table_stats_during_joins=0
    --enable_join_runtime_filters=0 --enable_parallel_replicas=0 --use_statistics=1
    --use_datalake_metadata_statistics=1 --use_iceberg_table_statistics_ndv=0 --explain_query_plan_default=legacy
    --use_iceberg_metadata_files_cache=0"

LAKE="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${LAKE}"
mkdir -p "${LAKE}"
cp -r "${CUR_DIR}/data_minio/table_statistics_db/ndv_table" "${LAKE}/ndv_table"
cp "${CUR_DIR}"/data_minio/table_statistics_db/ndv_table_column_sizes_only/*-m0.avro "${LAKE}/ndv_table/metadata/"

# Prints the numbers of distinct values of the `Estimated statistics` trace line. Usage: ndv [client flags].
ndv()
{
    ${CLICKHOUSE_CLIENT_TRACE} ${PINS} "$@" --query "
        EXPLAIN SELECT t.k, t.s, t.dec, t.d, t.sid, t.n FROM t JOIN dim ON t.k = dim.k" 2>&1 >/dev/null \
        | grep -oE "Estimated statistics for [A-Za-z]+ t: .*" | head -n 1 \
        | grep -oE '__table1\.[a-z]+: [0-9]+' | sed 's/__table1\.//' | sort
}

${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=0 --query "
    CREATE TABLE t ENGINE = IcebergLocal('${LAKE}/ndv_table');
    CREATE TABLE dim (k Int64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'uniq';
    INSERT INTO dim SELECT number FROM numbers(10);"

echo '--- From the sizes of the columns: k and sid do not compress and get none'
ndv --estimate_distinct_values_from_metadata=1

echo '--- Without: no column statistics'
ndv --estimate_distinct_values_from_metadata=0

${CLICKHOUSE_CLIENT} --query "DROP TABLE t; DROP TABLE dim;"
rm -rf "${LAKE}"
