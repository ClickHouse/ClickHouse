#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The lambda form of `einsum`: an alias inside an element of a tensor is rejected with an exception,
# because the array and the subscripts that are not indices are moved out of the lambda function.

$CLICKHOUSE_CLIENT -q "SELECT einsum((i) -> ((a[1] AS v)[i] + v[1])) FROM (SELECT [[1, 2], [3, 4]] AS a)" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'
$CLICKHOUSE_CLIENT -q "SELECT einsum(i -> (a AS v)[i] + 1) FROM (SELECT [1, 2] AS a)" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'
$CLICKHOUSE_CLIENT -q "SELECT einsum(i -> a[(1 AS k)][i] + k) FROM (SELECT [[1, 2], [3, 4]] AS a)" 2>&1 | grep -o -m1 'BAD_ARGUMENTS'

# Without the alias and with an alias of the whole element, it works.
$CLICKHOUSE_CLIENT -q "SELECT einsum((i) -> a[1][i] + a[1][1]) FROM (SELECT [[1, 2], [3, 4]] AS a)"
$CLICKHOUSE_CLIENT -q "SELECT einsum((i) -> (a[1][i] AS v) + v) FROM (SELECT [[1, 2], [3, 4]] AS a)"
