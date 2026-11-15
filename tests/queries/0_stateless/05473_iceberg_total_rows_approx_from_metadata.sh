#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg is not built in fast test.

# A read of an Iceberg table reports the `record_count` of the data files it reads, after pruning,
# as the total number of rows to read, so that the progress of the query is known.
# https://github.com/ClickHouse/ClickHouse/issues/128400

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}"
TABLE_PATH="${CLICKHOUSE_USER_FILES_UNIQUE}_iceberg/"
rm -rf "${TABLE_PATH}"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (p Int64, x Int64) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') PARTITION BY (p)"
# One data file per partition: 100, 200 and 300 rows.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg 1 --query "INSERT INTO ${TABLE} SELECT 1, number FROM numbers(100)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg 1 --query "INSERT INTO ${TABLE} SELECT 2, number FROM numbers(200)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg 1 --query "INSERT INTO ${TABLE} SELECT 3, number FROM numbers(300)"

# `X-ClickHouse-Summary` has the progress of the whole query, sent after it finishes with `wait_end_of_query`.
function total_rows_to_read()
{
    ${CLICKHOUSE_CURL} -sS -D - -o /dev/null "${CLICKHOUSE_URL}&wait_end_of_query=1&use_query_condition_cache=0&$2" -d "$1" \
        | grep -i '^X-ClickHouse-Summary' | grep -o '"total_rows_to_read":"[0-9]*"'
}

echo "all files"
total_rows_to_read "SELECT sum(x) FROM ${TABLE}"
echo "partition pruning"
total_rows_to_read "SELECT sum(x) FROM ${TABLE} WHERE p = 2" "use_iceberg_partition_pruning=1"
total_rows_to_read "SELECT sum(x) FROM ${TABLE} WHERE p >= 2" "use_iceberg_partition_pruning=1"
echo "parallel replicas"
total_rows_to_read "SELECT sum(x) FROM ${TABLE}" "enable_parallel_replicas=1&max_parallel_replicas=3&cluster_for_parallel_replicas=test_cluster_one_shard_three_replicas_localhost&parallel_replicas_for_object_storage_tables=1"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
rm -rf "${TABLE_PATH}"
