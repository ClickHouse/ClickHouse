#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_structure_hint_input_alias"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_structure_hint_input_alias (id UInt32, v UInt32) ENGINE = Memory"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&enable_analyzer=1" -d 'INSERT INTO t_structure_hint_input_alias SELECT i.id, i.v FROM input() AS i FORMAT JSONEachRow {"id":1,"v":10} {"id":3,"v":30}'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&enable_analyzer=1" -d 'INSERT INTO t_structure_hint_input_alias SELECT i.id, i.v FROM input() AS i WHERE i.id > 1 FORMAT JSONEachRow {"id":1,"v":10} {"id":3,"v":30}'

${CLICKHOUSE_CLIENT} --query "SELECT * FROM t_structure_hint_input_alias ORDER BY ALL"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_structure_hint_input_alias"
