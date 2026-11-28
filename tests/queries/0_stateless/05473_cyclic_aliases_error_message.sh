#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Print the part of the error message before the query, and the name of the error code.
function check()
{
    echo "$1"
    $CLICKHOUSE_LOCAL --query "$1" 2>&1 | grep -o -E '(Cyclic aliases: .*|Unknown [a-z ]* identifier .*|Multiple expressions .*)\. In scope|\(([A-Z_]+)\)$' | sed 's/\. In scope$//'
}

# Cycles through a column shadowed by an alias, in both orders of the projection.
check "SELECT number % 3 AS h3_3, h3_3 + 1 AS number FROM numbers(3)"
check "SELECT h3_3 + 1 AS number, number % 3 AS h3_3 FROM numbers(3)"

# Cycles without a column.
check "SELECT x + 1 AS y, y + 1 AS x"
check "WITH a + 1 AS b, b + 1 AS a SELECT a"
check "SELECT b + 1 AS a, c + 1 AS b, a + 1 AS c"

# Cycle of aliases of plain identifiers.
check "SELECT b AS a, a AS b"

# An expression outside of the cycle is not a part of it.
check "SELECT a + 1 AS z, b + 1 AS a, a + 1 AS b"

# Cycles through a nested scope.
check "SELECT arrayMap(y -> y + a, [1]) AS b, b AS a"
check "SELECT (SELECT a) AS b, b AS a"
check "SELECT arrayMap(y -> b, [1]) AS a, a + 1 AS b"

# An alias of a subexpression written in place is not a cycle, and neither is a reference to itself.
check "SELECT ((unknown_column + 1) AS y) + 1 AS unknown_column"
check "SELECT unknown_column + 1 AS unknown_column"

# The expression is valid if a cycle is not there.
$CLICKHOUSE_LOCAL --query "SELECT number + 1 AS number FROM numbers(1)"
$CLICKHOUSE_LOCAL --query "SELECT number % 3 AS x, x + 1 AS number FROM numbers(1) SETTINGS prefer_column_name_to_alias = 1"

# A duplicated alias is reported as such.
check "SELECT number AS num, num * 1 AS num FROM numbers(1)"
