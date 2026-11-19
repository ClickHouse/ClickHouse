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

# A column of another table in a JOIN is not hinted, it must not turn into a column of input().
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_structure_hint_input_join"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_structure_hint_input_join (id UInt32, v UInt32) ENGINE = Memory"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&enable_analyzer=1" -d 'INSERT INTO t_structure_hint_input_join SELECT i.id, g.v FROM input() AS i JOIN (SELECT 1 AS id2, 99 AS v) AS g ON i.id = g.id2 FORMAT JSONEachRow {"id":1,"v":10}'
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&enable_analyzer=1" -d 'INSERT INTO t_structure_hint_input_join SELECT id, g.v FROM input() AS i JOIN (SELECT 2 AS id2, 98 AS v) AS g ON id = g.id2 FORMAT JSONEachRow {"id":2,"v":20}'
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t_structure_hint_input_joined"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_structure_hint_input_joined (v UInt32) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_structure_hint_input_joined VALUES (97)"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&enable_analyzer=1" -d "INSERT INTO t_structure_hint_input_join SELECT i.id, ${CLICKHOUSE_DATABASE}.t_structure_hint_input_joined.v FROM input() AS i CROSS JOIN ${CLICKHOUSE_DATABASE}.t_structure_hint_input_joined FORMAT JSONEachRow {\"id\":3,\"v\":30}"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&enable_analyzer=1" -d 'INSERT INTO t_structure_hint_input_join WITH g AS (SELECT 96 AS v) SELECT i.id, g.v FROM input() AS i CROSS JOIN g FORMAT JSONEachRow {"id":4,"v":40}'

${CLICKHOUSE_CLIENT} --query "SELECT * FROM t_structure_hint_input_join ORDER BY ALL"
${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE t_structure_hint_input_join"

${CLICKHOUSE_CLIENT} --query "INSERT INTO t_structure_hint_input_join SELECT id, ${CLICKHOUSE_DATABASE}.t_structure_hint_input_joined.v FROM generateRandom(1, 10, 0) AS r CROSS JOIN ${CLICKHOUSE_DATABASE}.t_structure_hint_input_joined LIMIT 1"
${CLICKHOUSE_CLIENT} --query "SELECT v FROM t_structure_hint_input_join"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_structure_hint_input_join"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_structure_hint_input_joined"
