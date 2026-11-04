#!/usr/bin/env bash

# Checks that the JIT compiles a comparison of two dates of the same type, `Date`, `Date32` or `DateTime`, as the
# numbers that hold them, and that it returns what the comparison does without the JIT.
#
# The expressions compared are of comparisons of dates alone, which compiled into nothing before. The constant date
# differs between the runs of the test, so that the cache of compiled expressions does not have them already.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE jit_dates (d1 Date, d2 Date, t1 DateTime('UTC'), t2 DateTime('UTC'), e1 Date32, e2 Date32) ENGINE = MergeTree ORDER BY tuple();

    INSERT INTO jit_dates
    SELECT toDate('2020-01-01') + number % 100, toDate('2020-01-01') + number % 37,
           toDateTime('2020-01-01 00:00:00', 'UTC') + number * 1000, toDateTime('2020-01-01 00:00:00', 'UTC') + number * 777,
           toDate32('1950-01-01') + number % 50000 - 20000, toDate32('1950-01-01') + number % 40000 - 20000
    FROM numbers(100000);
"

DAYS=$((RANDOM % 1000))
EXPRESSION="(d1 < d2 AND t1 >= t2) OR e1 > e2 OR d1 = toDate('2020-01-01') + $DAYS"
# On one replica, so that the expressions are compiled by the query whose events are looked at.
QUERY="SELECT sum($EXPRESSION), sum(NOT ($EXPRESSION)) FROM jit_dates SETTINGS enable_parallel_replicas = 0"

ON_JIT=$($CLICKHOUSE_CLIENT --query_id "${CLICKHOUSE_TEST_UNIQUE_NAME}_jit" --query "$QUERY, compile_expressions = 1, min_count_to_compile_expression = 0")
WITHOUT_JIT=$($CLICKHOUSE_CLIENT --query "$QUERY, compile_expressions = 0")
if [ "$ON_JIT" == "$WITHOUT_JIT" ]; then
    echo "ok"
else
    echo "MISMATCH: $ON_JIT with the JIT, $WITHOUT_JIT without"
fi

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT --query "
    SELECT ProfileEvents['CompileFunction'] > 0
    FROM system.query_log
    WHERE current_database = currentDatabase() AND query_id = '${CLICKHOUSE_TEST_UNIQUE_NAME}_jit' AND type = 'QueryFinish'"

$CLICKHOUSE_CLIENT --query "DROP TABLE jit_dates"
