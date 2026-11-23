#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${CLICKHOUSE_TEST_UNIQUE_NAME}/"
trap 'rm -rf "${TABLE_PATH:?}"' EXIT

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (id Int64)
    ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
    SETTINGS output_format_parquet_row_group_size = 10000
"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --max_insert_threads=1 --max_block_size=10000 \
    --query "INSERT INTO ${TABLE} SELECT number FROM numbers(160000)"

for tasks_per_request in 0 1 16; do
    ${CLICKHOUSE_CLIENT} --log_comment="${CLICKHOUSE_TEST_UNIQUE_NAME}_${tasks_per_request}" --query "
        SELECT ${tasks_per_request}, count(), sum(id)
        FROM icebergLocalCluster('test_shard_localhost', '${TABLE_PATH}', 'Parquet')
        SETTINGS cluster_table_function_split_granularity = 'bucket', cluster_table_function_buckets_batch_size = 0,
            max_threads = 4, cluster_function_max_read_tasks_per_request = ${tasks_per_request}"
done

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

${CLICKHOUSE_CLIENT} --query "
    SELECT replaceOne(i.log_comment, '${CLICKHOUSE_TEST_UNIQUE_NAME}_', '') AS tasks_per_request,
        sum(q.ProfileEvents['ReadTaskRequestsSent'])
    FROM system.query_log AS q
    INNER JOIN
    (
        SELECT query_id, log_comment FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
            AND log_comment LIKE '${CLICKHOUSE_TEST_UNIQUE_NAME}\_%'
    ) AS i ON q.initial_query_id = i.query_id
    WHERE q.type = 'QueryFinish' AND NOT q.is_initial_query AND q.event_date >= yesterday()
    GROUP BY tasks_per_request
    ORDER BY toUInt64(tasks_per_request)"

${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
