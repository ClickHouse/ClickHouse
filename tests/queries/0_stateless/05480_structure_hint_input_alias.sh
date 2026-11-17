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

# A name with a dot, quoted or not, is a column of input(), not a subcolumn.
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_structure_hint_input_dotted"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_structure_hint_input_dotted (\`a.b\` UInt32, n Nested(a UInt32, b UInt32)) ENGINE = Memory"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&enable_analyzer=1" -d 'INSERT INTO t_structure_hint_input_dotted SELECT `a.b`, `n.a`, `n.b` FROM input() FORMAT JSONEachRow {"a.b":1,"n.a":[1,2],"n.b":[3,4]}'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&enable_analyzer=1" -d 'INSERT INTO t_structure_hint_input_dotted SELECT i.`a.b`, i.`n.a`, i.`n.b` FROM input() AS i FORMAT JSONEachRow {"a.b":2,"n.a":[5],"n.b":[6]}'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&enable_analyzer=1" -d 'INSERT INTO t_structure_hint_input_dotted SELECT a.b, n.a, n.b FROM input() FORMAT JSONEachRow {"a.b":3,"n.a":[7,8],"n.b":[9,10]}'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&enable_analyzer=1" -d 'INSERT INTO t_structure_hint_input_dotted SELECT i.a.b, i.n.a, i.n.b FROM input() AS i FORMAT JSONEachRow {"a.b":4,"n.a":[11],"n.b":[12]}'

${CLICKHOUSE_CLIENT} --query "SELECT * FROM t_structure_hint_input_dotted ORDER BY ALL"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_structure_hint_input_dotted"
