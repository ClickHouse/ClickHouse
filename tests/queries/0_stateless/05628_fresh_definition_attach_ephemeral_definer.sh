#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A view attached with a full definition keeps a definer that outlives an ephemeral user, like CREATE does.

ephemeral="ephemeral05628_${CLICKHOUSE_DATABASE}"
ephemeral_definer="${ephemeral}:definer"
db_engine=$(${CLICKHOUSE_CLIENT} -q "SELECT engine FROM system.databases WHERE name = currentDatabase()")
uuid_clause() { [ "${db_engine}" = "Ordinary" ] || echo "UUID '$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4()")'"; }

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS '${ephemeral}', '${ephemeral_definer}'"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${ephemeral} IN memory"

${CLICKHOUSE_CLIENT} -q "CREATE VIEW v_created DEFINER = ${ephemeral} SQL SECURITY DEFINER AS SELECT 1 AS x"
${CLICKHOUSE_CLIENT} --send_logs_level=error -q "ATTACH VIEW v_attached $(uuid_clause) (x UInt8) DEFINER = ${ephemeral} SQL SECURITY DEFINER AS SELECT 1 AS x"
${CLICKHOUSE_CLIENT} -q "SELECT name, countSubstrings(create_table_query, 'DEFINER = \`${ephemeral_definer}\`') FROM system.tables WHERE database = currentDatabase() ORDER BY name"

${CLICKHOUSE_CLIENT} -q "DROP USER ${ephemeral}"
${CLICKHOUSE_CLIENT} -q "SELECT name, x FROM (SELECT 'v_attached' AS name, x FROM v_attached UNION ALL SELECT 'v_created', x FROM v_created) ORDER BY name"

${CLICKHOUSE_CLIENT} -q "DROP TABLE v_created"
${CLICKHOUSE_CLIENT} -q "DROP TABLE v_attached"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS '${ephemeral_definer}'"
