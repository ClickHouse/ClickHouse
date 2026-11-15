#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs and S3 are not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan

# A distributed read of a Delta Lake table reports the `numRecords` of the data files as the total
# number of rows to read: the initiator passes the row counts to the replicas with the read tasks.
# https://github.com/ClickHouse/ClickHouse/issues/128400

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}"
TABLE_PATH="${CLICKHOUSE_DATABASE}_${RANDOM}_delta/"

DELTA_SETTINGS="--allow_experimental_delta_kernel_rs 1 --allow_delta_lake_writes 1 --allow_delta_lake_create_table 1"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
# shellcheck disable=SC2086
${CLICKHOUSE_CLIENT} ${DELTA_SETTINGS} --query "CREATE TABLE ${TABLE} (x Int64) ENGINE = DeltaLake(s3_conn, filename = '${TABLE_PATH}')"
# shellcheck disable=SC2086
${CLICKHOUSE_CLIENT} ${DELTA_SETTINGS} --query "INSERT INTO ${TABLE} SELECT number FROM numbers(100)"
# shellcheck disable=SC2086
${CLICKHOUSE_CLIENT} ${DELTA_SETTINGS} --query "INSERT INTO ${TABLE} SELECT number FROM numbers(200)"

function total_rows_to_read()
{
    ${CLICKHOUSE_CURL} -sS -D - -o /dev/null "${CLICKHOUSE_URL}&wait_end_of_query=1&use_query_condition_cache=0&allow_experimental_delta_kernel_rs=1&$2" \
        -d "$1" | grep -i '^X-ClickHouse-Summary' | grep -o '"total_rows_to_read":"[0-9]*"'
}

echo "parallel replicas"
total_rows_to_read "SELECT sum(x) FROM ${TABLE}" "enable_parallel_replicas=1&max_parallel_replicas=3&cluster_for_parallel_replicas=test_cluster_one_shard_three_replicas_localhost&parallel_replicas_for_object_storage_tables=1"
echo "deltaLakeCluster"
total_rows_to_read "SELECT sum(x) FROM deltaLakeCluster('test_cluster_one_shard_three_replicas_localhost', s3_conn, filename = '${TABLE_PATH}')"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
