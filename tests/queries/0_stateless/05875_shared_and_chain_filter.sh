#!/usr/bin/env bash
# Nested subqueries computing `c AND c` give a filter DAG with few nodes but 2^depth paths: each walk over it visits a node once.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# chain DEPTH SOURCE CONDITION [WHERE_SUFFIX]: nested subqueries over SOURCE, each computing `c AND c`.
chain() {
    local depth=$1 source=$2 condition=$3 suffix=${4:-} query
    query="WITH t0 AS (SELECT id, $condition AS c0 FROM $source)"
    for i in $(seq 1 "$depth"); do
        query="$query, t$i AS (SELECT id, c$((i - 1)) AND c$((i - 1)) AS c$i FROM t$((i - 1)))"
    done
    echo "$query SELECT count() FROM t$depth WHERE c$depth$suffix SETTINGS max_memory_usage = 200000000"
}

join="(SELECT r.id AS id, r.val AS val FROM (SELECT number AS id, number AS val FROM numbers(1000)) AS r INNER JOIN (SELECT number AS id FROM numbers(10)) AS l ON r.id = l.id)"
groups="(SELECT number % 20 AS id, count() AS n FROM numbers(1000) GROUP BY id)"

# Pushed below the JOIN.
${CLICKHOUSE_CLIENT} --query "$(chain 25 "$join" 'val < 4' ' AND id < 10')"
# Kept above the JOIN.
${CLICKHOUSE_CLIENT} --query "$(chain 25 "$join" 'val < 4' ' AND id < 10'), query_plan_filter_push_down = 0, compile_expressions = 0"
# The guard written first stays first in the filter pushed below the JOIN.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM (SELECT x, x != 0 AND x < 100 AS g FROM (SELECT l.x AS x FROM (SELECT number AS x FROM numbers(10)) AS l INNER JOIN (SELECT number AS y FROM numbers(10)) AS r ON l.x = r.y)) WHERE g AND intDiv(100, x) > 0 AND g SETTINGS short_circuit_function_evaluation = 'enable', short_circuit_function_evaluation_reorder_arguments = 0"
# Above an aggregation.
${CLICKHOUSE_CLIENT} --query "$(chain 25 "$groups" 'n > 0'), compile_expressions = 0"
