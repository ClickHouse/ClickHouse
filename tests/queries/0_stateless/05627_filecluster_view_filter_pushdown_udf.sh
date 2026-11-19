#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# A replica rebuilds the functions of the filter sent by the initiator through `FunctionFactory`, which has no WASM UDF.
# A conjunct with a deterministic WASM UDF stays on the initiator, and the rest of the filter is still sent.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

module_name="${CLICKHOUSE_TEST_UNIQUE_NAME}_module"
function_name="${CLICKHOUSE_TEST_UNIQUE_NAME}_identity"
file_name="${CLICKHOUSE_TEST_UNIQUE_NAME}_${RANDOM}.parquet"
view_name="filecluster_udf_view"
cluster_file="fileCluster('test_cluster_two_shards_localhost', '${file_name}', 'Parquet', 'id UInt64')"
query_id="${file_name%.parquet}"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP VIEW IF EXISTS ${view_name}"
    ${CLICKHOUSE_CLIENT} --query "DROP FUNCTION IF EXISTS ${function_name}"
    ${CLICKHOUSE_CLIENT} --query "DELETE FROM system.webassembly_modules WHERE name = '${module_name}'"
    rm -f "${USER_FILES_PATH}/${file_name}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} \
    --query "INSERT INTO system.webassembly_modules (name, code) SELECT '${module_name}', code FROM input('code String') FORMAT RawBlob" \
    < "${CUR_DIR}/wasm/identity_int.wasm"
${CLICKHOUSE_CLIENT} --query "
    CREATE FUNCTION ${function_name}
        LANGUAGE WASM FROM '${module_name}' :: 'identity_raw'
        ARGUMENTS (x Int32) RETURNS Int32
        ABI ROW_DIRECT
        DETERMINISTIC"

${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION file('${file_name}', 'Parquet', 'id UInt64') SELECT number FROM numbers(40000)"
${CLICKHOUSE_CLIENT} --query "CREATE VIEW ${view_name} AS SELECT id FROM ${cluster_file}"

${CLICKHOUSE_CLIENT} --query_id="${query_id}" --query "
    SELECT count(), sum(id) FROM ${view_name}
    WHERE ${function_name}(toInt32(id)) % 2 = 0 AND id BETWEEN 15000 AND 15099
    SETTINGS analyzer_inline_views = 0, log_queries = 1, log_queries_probability = 1"

# Every replica got a filter, and none of them got the UDF.
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT
        count() > 0 AND countIf(Settings['cluster_table_function_pushed_down_filter'] != '') = count(),
        countIf(position(tryBase64Decode(Settings['cluster_table_function_pushed_down_filter']), '${function_name}') > 0)
    FROM system.query_log
    WHERE event_date >= yesterday() AND type = 'QueryFinish' AND is_initial_query = 0
        AND initial_query_id = '${query_id}'
        AND initial_query_id IN (
            SELECT query_id FROM system.query_log
            WHERE event_date >= yesterday() AND current_database = currentDatabase() AND is_initial_query)
    SETTINGS enable_parallel_replicas = 0"
