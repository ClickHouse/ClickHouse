#!/usr/bin/env bash
# A client can send `cluster_table_function_pushed_down_filter` with a query that reaches a `fileCluster` replica
# through `remote` and a view. The replica decodes that filter within `input_format_binary_max_type_complexity`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

file_name="${CLICKHOUSE_TEST_UNIQUE_NAME}_${RANDOM}.parquet"
view_name="filecluster_type_complexity_view"
cluster_file="fileCluster('test_cluster_two_shards_localhost', '${file_name}', 'Parquet', 'id UInt64')"
query_id="${file_name%.parquet}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP VIEW IF EXISTS ${view_name}"
    rm -f "${USER_FILES_PATH}/${file_name}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION file('${file_name}', 'Parquet', 'id UInt64') SELECT number FROM numbers(1000)"
${CLICKHOUSE_CLIENT} --query "CREATE VIEW ${view_name} AS SELECT id FROM ${cluster_file}"

# Take a filter that the initiator sent to the replicas. It has an `Array(UInt64)` node, a type of complexity 2.
# The initiator sends a filter only when the replicas return the select list, as for the query of the view.
${CLICKHOUSE_CLIENT} --query_id="${query_id}" --query "
    SELECT count() FROM ${view_name} WHERE has([id, id + 1], 500)
    SETTINGS analyzer_inline_views = 0, log_queries = 1, log_queries_probability = 1"
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
filter=$(${CLICKHOUSE_CLIENT} --query "
    SELECT any(Settings['cluster_table_function_pushed_down_filter'])
    FROM system.query_log
    WHERE event_date >= yesterday() AND type = 'QueryFinish' AND is_initial_query = 0
        AND initial_query_id = '${query_id}'
        AND initial_query_id IN (
            SELECT query_id FROM system.query_log
            WHERE event_date >= yesterday() AND current_database = currentDatabase() AND is_initial_query)
    SETTINGS enable_parallel_replicas = 0")
[ -n "${filter}" ] || echo "The initiator did not send a filter"

# Send it from the client with a budget of 1.
${CLICKHOUSE_CLIENT} --query "
    SELECT sum(id) FROM remote('127.0.0.2', currentDatabase(), ${view_name})
    SETTINGS cluster_table_function_pushed_down_filter = '${filter}', input_format_binary_max_type_complexity = 1,
             serialize_query_plan = 0, enable_parallel_replicas = 0" 2>&1 \
    | grep -o -m1 'Binary type decoding complexity limit exceeded' || true
