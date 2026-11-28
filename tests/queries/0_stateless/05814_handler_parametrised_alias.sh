#!/usr/bin/env bash
# A SQL-defined HTTP handler binds the query parameter of an `AS {name:Identifier}` alias,
# and a view keeps that alias on a SQL user-defined function it expands. Both are global objects, named per database.
# Over HTTP, the query's own SETTINGS decide whether a view whose only parameters are such aliases is parameterized.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
H="h_alias_${CLICKHOUSE_DATABASE}"
P="/h_alias_${CLICKHOUSE_DATABASE}"
F="f_alias_${CLICKHOUSE_DATABASE}"

trap '$CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`$H\`; DROP VIEW IF EXISTS v_alias_udf; DROP FUNCTION IF EXISTS \`$F\`"' EXIT
$CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`$H\`; CREATE HANDLER \`$H\` URL '$P' AS SELECT number AS {a:Identifier} FROM numbers(2) FORMAT TSVWithNames"
$CLICKHOUSE_CLIENT -q "SELECT position(query, '{a:Identifier}') > 0 FROM system.handlers WHERE name = '$H'"
${CLICKHOUSE_CURL} -sS "${BASE}${P}?a=x"
${CLICKHOUSE_CURL} -sS "${BASE}${P}?param_a=y"

$CLICKHOUSE_CLIENT -q "DROP FUNCTION IF EXISTS \`$F\`; CREATE FUNCTION \`$F\` AS () -> 1; CREATE VIEW v_alias_udf AS SELECT \`$F\`() AS {a:Identifier}"
$CLICKHOUSE_CLIENT -q "SELECT parameterized_view_parameters FROM system.tables WHERE database = currentDatabase() AND name = 'v_alias_udf'"
$CLICKHOUSE_CLIENT -q "DETACH TABLE v_alias_udf; ATTACH TABLE v_alias_udf; SELECT * FROM v_alias_udf(a = 'z') FORMAT TSVWithNames"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary "CREATE VIEW v_alias_query_setting AS SELECT 7 AS {a:Identifier} SETTINGS parameterize_view_by_alias_parameter = 0, param_a = 'x'"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary "CREATE VIEW v_alias_query_compatibility AS SELECT 7 AS {a:Identifier} SETTINGS compatibility = '26.11', param_a = 'x'"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary "CREATE VIEW v_alias_query_bound_setting AS (SELECT 7 AS {a:Identifier}) SETTINGS max_threads = {threads:UInt64}, param_threads = '1', param_a = 'x'"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" --data-binary "CREATE VIEW v_alias_query_bound_flag AS (SELECT 7 AS {a:Identifier}) SETTINGS parameterize_view_by_alias_parameter = {flag:Bool}, param_flag = '0', param_a = 'x'"
$CLICKHOUSE_CLIENT -q "SELECT name, parameterized_view_parameters FROM system.tables WHERE database = currentDatabase() AND name LIKE 'v_alias_query_%' ORDER BY name"
$CLICKHOUSE_CLIENT -q "SELECT x FROM v_alias_query_setting; SELECT x FROM v_alias_query_compatibility; SELECT x FROM v_alias_query_bound_setting; SELECT x FROM v_alias_query_bound_flag"
