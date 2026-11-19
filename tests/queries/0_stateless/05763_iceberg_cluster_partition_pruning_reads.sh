#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and S3.

# `icebergS3Cluster` must not open the data files that partition pruning excludes.
# The check counts the files opened across the cluster, not the pruning metric, which was already non-zero
# when the files were read anyway. https://github.com/ClickHouse/ClickHouse/issues/80031

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLUSTER="test_cluster_two_shards_localhost"
FILENAME="${CLICKHOUSE_TEST_UNIQUE_NAME}/"
TF_SINGLE="icebergS3(s3_conn, filename = '${FILENAME}')"
TF_CLUSTER="icebergS3Cluster('${CLUSTER}', s3_conn, filename = '${FILENAME}')"

# Identity partition on `a`, one data file per partition.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    CREATE TABLE t (a Int32, b Float64) ENGINE = IcebergS3(s3_conn, filename = '${FILENAME}') PARTITION BY a;
    INSERT INTO t VALUES (1, 1.0), (2, 2.0), (3, 3.0), (4, 4.0), (5, 5.0);"

echo "--- the cluster result matches the single-node result, with and without pruning"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM ${TF_SINGLE} WHERE a = 1 ORDER BY ALL"
for prune in 0 1; do
    ${CLICKHOUSE_CLIENT} --use_iceberg_partition_pruning=${prune} --query "SELECT * FROM ${TF_CLUSTER} WHERE a = 1 ORDER BY ALL"
done

# The query condition cache would skip the files an earlier query found to have no match.
run()
{
    ${CLICKHOUSE_CLIENT} --query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_$1" --use_iceberg_partition_pruning="$3" --use_query_condition_cache=0 \
        --query "SELECT * FROM $2 WHERE a = 1 ORDER BY ALL FORMAT Null"
}
run cluster_no_pruning "${TF_CLUSTER}" 0
run cluster_pruning "${TF_CLUSTER}" 1
run single_pruning "${TF_SINGLE}" 1

echo "--- data files opened: all 5 without pruning, exactly 1 with pruning"
# The secondary queries of the cluster run in the default database; the query id prefix scopes them to this test.
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT replaceOne(initial_query_id, '${CLICKHOUSE_TEST_UNIQUE_NAME}_', '') AS name, sum(ProfileEvents['EngineFileLikeReadFiles'])
    FROM system.query_log
    WHERE (current_database = currentDatabase() OR NOT is_initial_query) AND event_date >= yesterday() AND type = 'QueryFinish'
        AND initial_query_id LIKE '${CLICKHOUSE_TEST_UNIQUE_NAME}\_%'
    GROUP BY name ORDER BY name"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t"
