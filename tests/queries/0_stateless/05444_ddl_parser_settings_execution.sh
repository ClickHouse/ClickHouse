#!/usr/bin/env bash
# Tags: distributed, replica, no-replicated-database
# Tag no-replicated-database: ON CLUSTER is not allowed

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

DB="ddl_parser_settings_execution_${CLICKHOUSE_TEST_UNIQUE_NAME}"
ZK_PATH="/clickhouse/databases/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/${DB}"

cleanup()
{
    for setting_source in query session; do
        ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.ddl_parser_local_${setting_source} SYNC" >/dev/null 2>&1 || true
        ${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none \
            --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.ddl_parser_cluster_${setting_source} ON CLUSTER test_shard_localhost SYNC" >/dev/null 2>&1 || true
    done
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB} SYNC" >/dev/null 2>&1 || true
    ${CLICKHOUSE_KEEPER_CLIENT} --query "rmr ${ZK_PATH}" >/dev/null 2>&1 || true
}

trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${DB} ENGINE = Replicated('${ZK_PATH}', '{shard}', '{replica}')"

for setting_source in query session; do
    settings_clause=""
    session_value=1
    if [[ ${setting_source} == query ]]; then
        session_value=0
        settings_clause="SETTINGS implicit_select = 1, allow_settings_after_format_in_insert = 1"
    fi

    for execution in local cluster replicated; do
        database=${CLICKHOUSE_DATABASE}
        on_cluster=""
        if [[ ${execution} == cluster ]]; then
            on_cluster="ON CLUSTER test_shard_localhost"
        elif [[ ${execution} == replicated ]]; then
            database=${DB}
        fi
        table="${database}.ddl_parser_${execution}_${setting_source}"

        ${CLICKHOUSE_CLIENT} --implicit_select="${session_value}" --allow_settings_after_format_in_insert="${session_value}" \
            --distributed_ddl_output_mode=throw --query "
                CREATE TABLE ${table} ${on_cluster} ENGINE = Memory AS SELECT
                    formatQueryOrNull('1 + 1') AS formatted,
                    JSONHas(parseQueryToJSON('INSERT INTO t SELECT 1 FORMAT Null SETTINGS max_threads = 1'), 'settings_ast') AS parsed_settings
                ${settings_clause}" >/dev/null
        ${CLICKHOUSE_CLIENT} --query "SELECT '${setting_source}', '${execution}', formatted, parsed_settings FROM ${table}"
    done
done
