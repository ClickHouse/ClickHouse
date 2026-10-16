#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query="WITH prometheus_query_step_1 AS MATERIALIZED (SELECT 1 AS x, 2 AS y)"
for i in $(seq 2 22); do
    prev=$((i - 1))
    query+=", prometheus_query_step_${i} AS MATERIALIZED (SELECT l.x AS x, r.y AS y FROM prometheus_query_step_${prev} AS l INNER JOIN prometheus_query_step_${prev} AS r ON l.x = r.x)"
done
query+=" SELECT x, y FROM prometheus_query_step_22"

${CLICKHOUSE_CLIENT} --enable_analyzer 1 --enable_materialized_cte 1 --query "${query}"

explain_lines=$(${CLICKHOUSE_CLIENT} --enable_analyzer 1 --enable_materialized_cte 1 --query "EXPLAIN QUERY TREE dump_tree = 0, dump_ast = 1, passes = 4 ${query}" | wc -l)
${CLICKHOUSE_CLIENT} --query "SELECT ${explain_lines} < 80"

${CLICKHOUSE_CLIENT} --enable_analyzer 1 --enable_materialized_cte 1 --query "
WITH prometheus_query_step_1 AS MATERIALIZED (SELECT 1 AS x, 10 AS y)
SELECT p.x, q.y
FROM prometheus_query_step_1 AS p
CROSS JOIN prometheus_query_step_1 AS q"
