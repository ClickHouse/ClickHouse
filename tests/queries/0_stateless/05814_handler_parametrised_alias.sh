#!/usr/bin/env bash
# A SQL-defined HTTP handler binds the query parameter of an `AS {name:Identifier}` alias,
# and a view keeps that alias on a SQL user-defined function it expands. Both are global objects, named per database.

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
