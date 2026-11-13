#!/usr/bin/env bash
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

file_name="${CLICKHOUSE_TEST_UNIQUE_NAME}_${RANDOM}.parquet"
view_name="filecluster_filter_view"
cluster_file="fileCluster('test_cluster_two_shards_localhost', '${file_name}', 'Parquet', 'id UInt64')"
query_id="${file_name%.parquet}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP VIEW IF EXISTS ${view_name}"
    rm -f "${USER_FILES_PATH}/${file_name}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${file_name}', 'Parquet', 'id UInt64')
    SELECT number FROM numbers(40000)
    SETTINGS max_threads = 1, max_block_size = 10000, output_format_parquet_row_group_size = 10000"
${CLICKHOUSE_CLIENT} --query "CREATE VIEW ${view_name} AS SELECT id FROM ${cluster_file}"

${CLICKHOUSE_CLIENT} --query_id="${query_id}_direct" --query "
    SELECT count(), sum(id) FROM ${cluster_file} WHERE id BETWEEN 15000 AND 15099
    SETTINGS input_format_parquet_filter_push_down = 1, use_query_condition_cache = 0,
             log_queries = 1, log_queries_probability = 1"
${CLICKHOUSE_CLIENT} --query_id="${query_id}_view" --query "
    SELECT count(), sum(id) FROM ${view_name} WHERE id BETWEEN 15000 AND 15099
    SETTINGS analyzer_inline_views = 0, input_format_parquet_filter_push_down = 1, use_query_condition_cache = 0,
             log_queries = 1, log_queries_probability = 1"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT
        extract(initial_query_id, '(direct|view)$'),
        sum(ProfileEvents['ParquetReadRowGroups']) > 0,
        sum(ProfileEvents['ParquetPrunedRowGroups']) > 0,
        sum(read_rows) < 40000
    FROM system.query_log
    WHERE type = 'QueryFinish' AND is_initial_query = 0
        AND initial_query_id IN ('${query_id}_direct', '${query_id}_view')
    GROUP BY initial_query_id
    ORDER BY initial_query_id
    SETTINGS enable_parallel_replicas = 0"
