#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Join reordering checks whether a join key is constant through the subqueries it comes from. Here every column of
# each subquery depends on every column of the one below, so the check must take each column once, or it never ends.
# The chain is wide rather than deep: under TSan about 20 nested subqueries exhaust the allowed stack.
width=8
depth=12
columns() { local sep="" j; for ((j = 0; j < width; ++j)); do printf '%sc%d_%d' "$sep" "$1" "$j"; sep=", "; done; }

select=""
for ((j = 0; j < width; ++j)); do select+="materialize(toUInt64($j)) AS c0_$j, "; done
query="SELECT ${select%, } FROM numbers(10)"
for ((i = 1; i <= depth; ++i)); do
    select=""
    for ((j = 0; j < width; ++j)); do select+="cityHash64($j, $(columns $((i - 1)))) AS c${i}_$j, "; done
    query="SELECT ${select%, } FROM ($query) LIMIT 1 BY $(columns "$i")"
done

# A runtime filter on the join key would be pushed down through every LIMIT BY, which is not what this test checks.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM ($query) AS p JOIN numbers(10) AS t ON p.c${depth}_0 = t.number
    SETTINGS enable_join_transitive_predicates = 1, query_plan_optimize_join_order_limit = 10, enable_join_runtime_filters = 0"
