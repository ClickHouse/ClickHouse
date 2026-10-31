#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Deeply nested `unique` calls must be parsed in reasonable time, whether they are the `UNIQUE` predicate
# or ordinary function calls whose argument starts with a subquery. Before the fix, the argument of every
# ordinary call `unique((SELECT ...), 2)` was parsed as a subquery and then again as an expression,
# which doubled the work at every nesting level.
# See https://github.com/ClickHouse/ClickHouse/pull/99877

nested()
{
    local expr="1"
    for _ in $(seq 1 20)
    do
        expr="$1$expr$2"
    done
    echo "SELECT $expr"
}

# Prints the number of `UNIQUE` predicates and of ordinary `unique` calls in the formatted query.
count()
{
    local formatted
    formatted=$($CLICKHOUSE_FORMAT --oneline --query "$(nested "$1" "$2")")
    echo "$(grep -o 'UNIQUE(' <<< "$formatted" | wc -l) $(grep -o 'unique(' <<< "$formatted" | wc -l)"
}

count 'unique((SELECT ' '))'
count 'unique(SELECT ' ')'
count 'unique((SELECT ' '), 2)'
count 'unique((SELECT ' ') + 1)'
count 'unique((SELECT unique((SELECT ' '), 2)))'
count 'unique(((SELECT ' ')) UNION ALL SELECT 2)'
