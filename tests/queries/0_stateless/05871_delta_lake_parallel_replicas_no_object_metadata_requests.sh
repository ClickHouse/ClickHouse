#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs and MinIO are not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan

# With parallel replicas, the initiator passes the size that the `add` action of a Delta Lake data file records to
# the replicas with the read tasks, so no replica requests the object metadata (S3 HEAD) of the Parquet data files.
# Each INSERT adds one data file, so the requests of all the queries of a read are compared between 3 and 6 files.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The reads go through the distributed cache by default where it is enabled, but the test checks the reads of this server.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --table_engine_read_through_distributed_cache 0"

TABLE="t_${CLICKHOUSE_DATABASE}"
TABLE_PATH="${CLICKHOUSE_TEST_UNIQUE_NAME}/t0"
Q="${CLICKHOUSE_TEST_UNIQUE_NAME}_"
DELTA_SETTINGS="--allow_experimental_delta_kernel_rs 1 --allow_delta_lake_writes 1 --allow_delta_lake_create_table 1"
PARALLEL_REPLICAS_SETTINGS="--allow_experimental_delta_kernel_rs 1 --enable_parallel_replicas 1 --max_parallel_replicas 3 --cluster_for_parallel_replicas test_cluster_one_shard_three_replicas_localhost --parallel_replicas_for_object_storage_tables 1 --use_query_condition_cache 0"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
# shellcheck disable=SC2086
${CLICKHOUSE_CLIENT} ${DELTA_SETTINGS} --query "CREATE TABLE ${TABLE} (c0 Int32, c1 String) ENGINE = DeltaLakeS3(s3_conn, filename = '${TABLE_PATH}')"

insert()
{
    for i in "$@"; do
        # shellcheck disable=SC2086
        ${CLICKHOUSE_CLIENT} ${DELTA_SETTINGS} --query "INSERT INTO ${TABLE} VALUES ($i, toString($i))"
    done
}

insert 1 2 3
# shellcheck disable=SC2086
${CLICKHOUSE_CLIENT} ${PARALLEL_REPLICAS_SETTINGS} --query_id "${Q}3" --query "SELECT sum(c0) FROM ${TABLE}"
insert 4 5 6
# shellcheck disable=SC2086
${CLICKHOUSE_CLIENT} ${PARALLEL_REPLICAS_SETTINGS} --query_id "${Q}6" --query "SELECT sum(c0) FROM ${TABLE}"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

${CLICKHOUSE_CLIENT} --query "
    SELECT
        uniqExactIf(query_id, initial_query_id = '${Q}6' AND NOT is_initial_query) > 0,
        sumIf(ProfileEvents['S3HeadObject'], initial_query_id = '${Q}6') - sumIf(ProfileEvents['S3HeadObject'], initial_query_id = '${Q}3')
    FROM system.query_log
    WHERE type = 'QueryFinish' AND initial_query_id IN ('${Q}3', '${Q}6')
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
