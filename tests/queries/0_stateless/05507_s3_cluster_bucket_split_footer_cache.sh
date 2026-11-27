#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs S3 (MinIO)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="http://localhost:11111/test/${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

${CLICKHOUSE_CLIENT} --output_format_parquet_row_group_size=10000 --output_format_parquet_row_group_size_bytes=100000 --max_insert_threads=1 \
    --query "INSERT INTO FUNCTION s3('${FILE}', 'test', 'testtest', 'Parquet') SELECT number AS id, toString(number) AS s FROM numbers(100000) SETTINGS s3_truncate_on_insert = 1"

ROW_GROUPS=$(${CLICKHOUSE_CLIENT} --query "SELECT num_row_groups FROM s3('${FILE}', 'test', 'testtest', 'ParquetMetadata')")
${CLICKHOUSE_CLIENT} --query "SELECT ${ROW_GROUPS} > 1"

for run in cold warm; do
    query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_${run}_${RANDOM}"
    ${CLICKHOUSE_CLIENT} --query_id "${query_id}" --query "
        SELECT count(), sum(id) FROM s3Cluster('test_cluster_two_shards_localhost', '${FILE}', 'test', 'testtest', 'Parquet', 'id UInt64, s String')
        SETTINGS cluster_table_function_split_granularity = 'bucket', cluster_table_function_buckets_batch_size = 0,
            cluster_table_function_split_by_split_offsets = 0, use_parquet_metadata_cache = 1, use_query_condition_cache = 0"
    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    ${CLICKHOUSE_CLIENT} --query "
        SELECT '${run}', sum(ProfileEvents['ParquetMetadataCacheMisses']), toInt64(sum(ProfileEvents['ParquetMetadataCacheHits'])) - ${ROW_GROUPS}
        FROM system.query_log
        WHERE event_date >= yesterday() AND type = 'QueryFinish'
            AND initial_query_id IN (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase() AND query_id = '${query_id}')"
done
