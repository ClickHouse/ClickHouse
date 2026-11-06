#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

with_clause="WITH prometheus_query_step_1 AS MATERIALIZED (SELECT 1 AS x, 2 AS y)"
for i in $(seq 2 28); do
    prev=$((i - 1))
    with_clause+=", prometheus_query_step_${i} AS MATERIALIZED (SELECT l.x AS x, r.y AS y FROM prometheus_query_step_${prev} AS l INNER JOIN prometheus_query_step_${prev} AS r ON l.x = r.x)"
done
query="${with_clause} SELECT x, y FROM prometheus_query_step_28"

${CLICKHOUSE_CLIENT} --enable_analyzer 1 --enable_materialized_cte 1 --query "${query}"

explain_lines=$(${CLICKHOUSE_CLIENT} --enable_analyzer 1 --enable_materialized_cte 1 --query "EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1, passes = 4 ${query}" | wc -l)
${CLICKHOUSE_CLIENT} --query "SELECT ${explain_lines} < 80"

${CLICKHOUSE_CLIENT} --enable_analyzer 1 --enable_materialized_cte 1 --query "
WITH prometheus_query_step_1 AS MATERIALIZED (SELECT 1 AS x, 10 AS y)
SELECT p.x, q.y
FROM prometheus_query_step_1 AS p
CROSS JOIN prometheus_query_step_1 AS q"

${CLICKHOUSE_CLIENT} --enable_analyzer 1 --enable_materialized_cte 1 --max_execution_time 30 --query "${query} WHERE x = (SELECT max(x) FROM prometheus_query_step_28)"

${CLICKHOUSE_CLIENT} --enable_analyzer 1 --enable_materialized_cte 1 --convert_query_to_cnf 1 --query "${query} WHERE x IN (SELECT x FROM prometheus_query_step_28)"

explain_tree_lines=$(${CLICKHOUSE_CLIENT} --enable_analyzer 1 --enable_materialized_cte 1 --query "EXPLAIN QUERY TREE ${query}" | wc -l)
${CLICKHOUSE_CLIENT} --query "SELECT ${explain_tree_lines} < 2000"

in_query="WITH prometheus_query_step_1 AS MATERIALIZED (SELECT 1 AS x)"
for i in $(seq 2 20); do
    prev=$((i - 1))
    in_query+=", prometheus_query_step_${i} AS MATERIALIZED (SELECT x FROM prometheus_query_step_${prev} WHERE x IN (SELECT x FROM prometheus_query_step_${prev}))"
done
in_query+=" SELECT x FROM prometheus_query_step_20"

${CLICKHOUSE_CLIENT} --enable_analyzer 1 --enable_materialized_cte 1 --query "${in_query}"

explain_in_lines=$(${CLICKHOUSE_CLIENT} --enable_analyzer 1 --enable_materialized_cte 1 --query "EXPLAIN QUERY TREE dump_tree = 1, dump_ast = 0, passes = 1 ${in_query}" | wc -l)
${CLICKHOUSE_CLIENT} --query "SELECT ${explain_in_lines} < 2000"

${CLICKHOUSE_CLIENT} --enable_analyzer 1 --enable_materialized_cte 1 --query "${with_clause}, number IN (SELECT x FROM prometheus_query_step_28) AS a SELECT arrayMap(z -> a, [1]) FROM numbers(3)"
