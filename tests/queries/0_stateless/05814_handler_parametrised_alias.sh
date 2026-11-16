#!/usr/bin/env bash
# A SQL-defined HTTP handler binds the query parameter of an `AS {name:Identifier}` alias.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE="${CLICKHOUSE_PORT_HTTP_PROTO}://${CLICKHOUSE_HOST}:${CLICKHOUSE_PORT_HTTP}"
H="h_alias_${CLICKHOUSE_DATABASE}"
P="/h_alias_${CLICKHOUSE_DATABASE}"

trap '$CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`$H\`"' EXIT
$CLICKHOUSE_CLIENT -q "DROP HANDLER IF EXISTS \`$H\`; CREATE HANDLER \`$H\` URL '$P' AS SELECT number AS {a:Identifier} FROM numbers(2) FORMAT TSVWithNames"
$CLICKHOUSE_CLIENT -q "SELECT position(query, '{a:Identifier}') > 0 FROM system.handlers WHERE name = '$H'"
${CLICKHOUSE_CURL} -sS "${BASE}${P}?a=x"
${CLICKHOUSE_CURL} -sS "${BASE}${P}?param_a=y"
