#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Join reordering checks whether a join key is constant through the subqueries it comes from. Here both columns of
# each subquery depend on both columns of the one below, so the check must take each column once, or it never ends.
query="SELECT materialize(toUInt64(1)) AS a0, materialize(toUInt64(2)) AS b0 FROM numbers(10)"
for i in $(seq 1 40); do
    query="SELECT a$((i - 1)) + b$((i - 1)) AS a$i, bitXor(a$((i - 1)), b$((i - 1))) AS b$i FROM ($query) LIMIT 1 BY a$i, b$i"
done

# A runtime filter on `a40` would be pushed down through every LIMIT BY, which is not what this test checks.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM ($query) AS p JOIN numbers(10) AS t ON p.a40 = t.number
    SETTINGS enable_join_transitive_predicates = 1, query_plan_optimize_join_order_limit = 10, enable_join_runtime_filters = 0"
