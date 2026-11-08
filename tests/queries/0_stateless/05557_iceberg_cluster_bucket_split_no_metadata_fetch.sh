#!/usr/bin/env bash
# Tags: no-fasttest

# The pieces of an Iceberg data file sent to the replicas with `cluster_table_function_split_granularity = 'bucket'`
# must carry the file size from the manifest, so that no task fetches the metadata of its object again.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/"
trap 'rm -rf "${TABLE_PATH:?}"' EXIT

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (id Int64, s String)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    SETTINGS output_format_parquet_row_group_size = 10000"

for i in 0 1; do
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --max_insert_threads=1 \
        --query "INSERT INTO ${TABLE} SELECT number + ${i} * 50000, toString(number) FROM numbers(50000)"
done

for granularity in file bucket; do
    echo "--- ${granularity}"
    ${CLICKHOUSE_CLIENT} --log_comment="${CLICKHOUSE_TEST_UNIQUE_NAME}_${granularity}" \
        --enable_parallel_replicas=1 --max_parallel_replicas=2 --cluster_for_parallel_replicas='test_cluster_two_shards_localhost' \
        --parallel_replicas_for_cluster_engines=1 --cluster_table_function_split_granularity="${granularity}" --cluster_table_function_buckets_batch_size=0 \
        --query "SELECT count(), sum(id) FROM ${TABLE}"
done

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

echo "--- tasks, metadata fetched on replicas"
${CLICKHOUSE_CLIENT} --query "
    SELECT i.granularity, countIf(q.is_initial_query = 0) > 0, sumIf(q.ProfileEvents['ObjectStorageMetadataFetchedForRead'], q.is_initial_query = 0)
    FROM system.query_log AS q
    INNER JOIN
    (
        SELECT query_id, replaceOne(log_comment, '${CLICKHOUSE_TEST_UNIQUE_NAME}_', '') AS granularity FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
            AND log_comment LIKE '${CLICKHOUSE_TEST_UNIQUE_NAME}\_%'
    ) AS i ON q.initial_query_id = i.query_id
    WHERE q.type = 'QueryFinish' AND q.event_date >= yesterday()
    GROUP BY i.granularity ORDER BY i.granularity"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
