#!/usr/bin/env bash
# The initiator sends a set of the filter on a view over `fileCluster` to every replica, so the set has to fit
# `max_rows_to_transfer` and `max_bytes_to_transfer`. A larger set stays on the initiator, with the same result.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

file_name="${CLICKHOUSE_TEST_UNIQUE_NAME}_${RANDOM}.parquet"
view_name="filecluster_transfer_limits_view"
cluster_file="fileCluster('test_cluster_two_shards_localhost', '${file_name}', 'Parquet', 'id UInt64')"
query_id="${file_name%.parquet}"
in_list=$(seq -s, 15000 15099)

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP VIEW IF EXISTS ${view_name}"
    rm -f "${USER_FILES_PATH}/${file_name}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION file('${file_name}', 'Parquet', 'id UInt64') SELECT number FROM numbers(40000)"
${CLICKHOUSE_CLIENT} --query "CREATE VIEW ${view_name} AS SELECT id FROM ${cluster_file}"

for limits in "unlimited:max_rows_to_transfer = 0, max_bytes_to_transfer = 0" "limited:max_rows_to_transfer = 10"; do
    ${CLICKHOUSE_CLIENT} --query_id="${query_id}_${limits%%:*}" --query "
        SELECT count(), sum(id) FROM ${view_name} WHERE id IN (${in_list}) AND id >= 10000
        SETTINGS ${limits#*:}, analyzer_inline_views = 0, log_queries = 1, log_queries_probability = 1"
done

# With the limit, the replicas still get `id >= 10000`, but not the set of 100 rows.
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT
        minIf(length(filter), limited) > 0,
        minIf(length(filter), NOT limited) > maxIf(length(filter), limited)
    FROM
    (
        SELECT endsWith(initial_query_id, '_limited') AS limited, Settings['cluster_table_function_pushed_down_filter'] AS filter
        FROM system.query_log
        WHERE event_date >= yesterday() AND type = 'QueryFinish' AND is_initial_query = 0
            AND initial_query_id IN ('${query_id}_unlimited', '${query_id}_limited')
            AND initial_query_id IN (
                SELECT query_id FROM system.query_log
                WHERE event_date >= yesterday() AND current_database = currentDatabase() AND is_initial_query)
    )
    SETTINGS enable_parallel_replicas = 0"
