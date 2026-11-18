#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# clickhouse-local with a system log on a persistent path keeps the system log table between runs.

dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
config="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.xml"
rm -rf "${dir}"
echo "<clickhouse><query_log><database>system</database><table>query_log</table></query_log></clickhouse>" > "${config}"

for run in 1 2; do
    $CLICKHOUSE_LOCAL --config-file "${config}" --path "${dir}" -q "
        SELECT 'run ${run}' FORMAT Null;
        SYSTEM FLUSH LOGS query_log;
        SELECT engine FROM system.databases WHERE name = 'system';
        SELECT DISTINCT query FROM system.query_log WHERE current_database = currentDatabase() AND query LIKE 'SELECT \'run %' AND type = 'QueryFinish' ORDER BY query;
    "
done

# A directory used by an older version, with the data of a system log table in `data/system` but without
# `metadata/system.sql`, keeps the `Memory` engine for the `system` database instead of failing to start.
rm -rf "${dir}"
$CLICKHOUSE_LOCAL --path "${dir}" -q "SELECT 1 FORMAT Null"
mkdir -p "${dir}/data/system/query_log"
for run in 1 2; do
    $CLICKHOUSE_LOCAL --config-file "${config}" --path "${dir}" -q "SELECT engine FROM system.databases WHERE name = 'system'"
done

rm -rf "${dir}" "${config}"
