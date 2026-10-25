#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="05441_iceberg_cluster/${CLICKHOUSE_TEST_UNIQUE_NAME}"
LOCAL_DIR="${CLICKHOUSE_TEST_UNIQUE_NAME}_local"
trap 'rm -rf "${USER_FILES_PATH:?}/${LOCAL_DIR}"' EXIT

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE} (c0 Int32) ENGINE = IcebergS3(s3_conn, filename = '${TABLE_PATH}')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1), (2), (3)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (4)"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE}_local (c0 Int32) ENGINE = IcebergLocal('${USER_FILES_PATH}/${LOCAL_DIR}/')"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE}_local VALUES (1), (2), (3)"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE}_local VALUES (4)"

# Replicas must read the metadata file the initiator resolved instead of listing the metadata directory.
${CLICKHOUSE_CLIENT} --log_comment="${CLICKHOUSE_TEST_UNIQUE_NAME}_cluster" --iceberg_metadata_staleness_ms=0 --query "
    SELECT sum(c0) FROM icebergS3Cluster('test_cluster_two_shards_localhost', s3_conn, filename = '${TABLE_PATH}')"

${CLICKHOUSE_CLIENT} --log_comment="${CLICKHOUSE_TEST_UNIQUE_NAME}_converted" --iceberg_metadata_staleness_ms=0 \
    --enable_parallel_replicas=1 --max_parallel_replicas=2 --cluster_for_parallel_replicas='test_cluster_two_shards_localhost' \
    --parallel_replicas_for_cluster_engines=1 --query "SELECT sum(c0) FROM icebergS3(s3_conn, filename = '${TABLE_PATH}')"

${CLICKHOUSE_CLIENT} --log_comment="${CLICKHOUSE_TEST_UNIQUE_NAME}_local" --iceberg_metadata_staleness_ms=0 --query "
    SELECT sum(c0) FROM icebergLocalCluster('test_cluster_two_shards_localhost', '${USER_FILES_PATH}/${LOCAL_DIR}/', 'Parquet')"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

replica_queries="
    system.query_log AS q
    INNER JOIN
    (
        SELECT query_id, replaceOne(log_comment, '${CLICKHOUSE_TEST_UNIQUE_NAME}_', '') AS arm FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
            AND log_comment LIKE '${CLICKHOUSE_TEST_UNIQUE_NAME}\_%'
    ) AS i ON q.initial_query_id = i.query_id
    WHERE q.type = 'QueryFinish' AND q.event_date >= yesterday()"

# The replicas ran and listed nothing, while the initiator resolved the latest metadata file by listing.
${CLICKHOUSE_CLIENT} --query "
    SELECT i.arm, countIf(q.is_initial_query = 0) > 0, sumIf(q.ProfileEvents['S3ListObjects'], q.is_initial_query = 0),
        sumIf(q.ProfileEvents['S3ListObjects'], q.is_initial_query = 1) > 0
    FROM ${replica_queries} AND i.arm != 'local'
    GROUP BY i.arm ORDER BY i.arm"

# The local object storage has no listing counter: check that the replicas received the initiator's current metadata file.
${CLICKHOUSE_CLIENT} --query "
    SELECT i.arm, countIf(q.is_initial_query = 0) > 0,
        countIf(q.is_initial_query = 0 AND position(q.query, 'iceberg_metadata_file_path = \'metadata/v3.metadata.json\'') > 0) = countIf(q.is_initial_query = 0)
    FROM ${replica_queries} AND i.arm = 'local'
    GROUP BY i.arm"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}_local"
