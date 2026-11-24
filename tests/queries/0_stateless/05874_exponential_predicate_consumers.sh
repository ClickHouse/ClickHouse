#!/usr/bin/env bash
# Nested subqueries that double a column give a filter DAG with few nodes but 2^depth paths: every planner
# consumer of such a filter (names, index analysis, PREWHERE, JIT, lazy execution, JOIN push-down) must stay linear.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# chain LEVELS STEPS SEED PASS FROM [OP]: LEVELS nested subqueries over FROM; each one combines (OP, default +) the
# previous level's last column with itself STEPS times (a<level>_<step>) and passes the PASS columns through.
chain() {
    local levels=$1 steps=$2 prev=$3 pass=$4 query=$5 op=${6:-+} projection
    for i in $(seq 1 "$levels"); do
        projection="$prev $op $prev AS a${i}_1"
        for j in $(seq 2 "$steps"); do
            projection="$projection, a${i}_$((j - 1)) $op a${i}_$((j - 1)) AS a${i}_$j"
        done
        query="(SELECT $projection$pass FROM $query)"
        prev="a${i}_$steps"
    done
    echo "$query"
}

run() {
    ${CLICKHOUSE_CLIENT} --query "$1 SETTINGS max_memory_usage = 200000000${2:-}"
}

# 24 doublings: on a consumer walking paths these run out of memory.
numbers_chain=$(chain 4 6 val '' '(SELECT number - 1 AS val FROM numbers(1, 1))')
run "SELECT count() FROM $numbers_chain WHERE a4_6 <= 0"
run "SELECT count() FROM $numbers_chain WHERE a4_6 <= 0" ', query_plan_merge_expressions = 0'
run "SELECT count() FROM $numbers_chain WHERE a4_6 <= 0" ', compile_expressions = 1, min_count_to_compile_expression = 0'
run "SELECT count() FROM $(chain 4 6 val '' '(SELECT number - 1 AS val FROM numbers(1, 1))' '=') WHERE a4_6"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_pad (val UInt64, pad String) ENGINE = MergeTree ORDER BY tuple()"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_pad VALUES (0, 'x')"
run "SELECT count(), max(pad) FROM $(chain 4 6 val ', pad' t_pad) WHERE a4_6 = 0"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_marks (val UInt64, pad String) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_marks SELECT 0, 'x' FROM numbers(100)"
run "SELECT count(), max(pad) FROM $(chain 4 6 val ', v, pad' '(SELECT val, val AS v, pad FROM t_marks)') WHERE v < 5 AND a4_6 = 0"

${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_final (k UInt64, val UInt64) ENGINE = ReplacingMergeTree ORDER BY k"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_final VALUES (0, 0)"
run "SELECT count() FROM $(chain 4 6 k ', val' 't_final FINAL') WHERE a4_6 = 0"

run "SELECT count() FROM $(chain 4 6 val ', n' '(SELECT number AS n, number - 1 AS val FROM numbers(1, 1))') WHERE n < 0 OR a4_6 <= 0"

run "SELECT count() FROM $(chain 4 6 a0 '' '(SELECT l.x AS a0 FROM (SELECT number AS x FROM numbers(1)) AS l INNER JOIN (SELECT number AS y FROM numbers(1)) AS r ON l.x = r.y)') WHERE a4_6 <= 0"

join_chain=$(chain 4 6 r.val '' '(SELECT number AS id, number - 1 AS val FROM numbers(1, 1)) AS r LEFT JOIN (SELECT number AS id FROM numbers(2)) AS l ON l.id = r.id')
run "SELECT count() FROM $join_chain AS g INNER JOIN (SELECT 0 AS val) AS k ON g.a4_6 = k.val AND g.a4_6 <= k.val"

# 40 doublings: on a consumer walking paths these take hours.
join_two_columns='(SELECT l.val AS val, r.y AS y FROM (SELECT number - 1 AS k, number - 1 AS val FROM numbers(1, 1)) AS l INNER JOIN (SELECT number AS y FROM numbers(2)) AS r ON l.k = r.y)'
run "SELECT count() FROM $(chain 5 8 val ', y' "$join_two_columns") WHERE a5_8 <= 0 OR y = 1"
run "SELECT count() FROM $(chain 5 8 'materialize(1)' ', val, y' "$join_two_columns") WHERE a5_8 <= 0 OR val + y = 7"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_pad"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_marks"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_final"
