#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas, no-fasttest
# Regression: an aggregate function that checks access rights when it is created (`flameGraph`
# requires the `INTROSPECTION` grant) can be named by a string argument of an ordinary function.
# `arrayReduce('flameGraph', ...)` and `initializeAggregation('flameGraph', ...)` with constant
# arguments are folded into a constant during analysis, so a query plan cache hit would not create
# the aggregate function again and would keep serving the value after the grant was revoked.
# The query plan cache is a single, server-wide cache that this test inspects via the global
# `SYSTEM DROP QUERY PLAN CACHE` and exact `QueryPlanCacheHits` counts, and the test creates a global
# user (no-parallel). Whether a second run hits depends on the exact logical plan, which random
# settings perturb. The cache only engages with the analyzer and is incompatible with parallel
# replicas. Introspection functions are not available in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05843_${CLICKHOUSE_DATABASE}"
SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --allow_introspection_functions=1"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_05843;
    CREATE TABLE t_05843 (id UInt64) ENGINE = Memory;
    INSERT INTO t_05843 VALUES (1);

    DROP USER IF EXISTS $user;
    CREATE USER $user;
    REVOKE ALL ON *.* FROM $user;
    GRANT SELECT ON t_05843 TO $user;
"

run_user()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT --user="$user" $SETTINGS --query "$1" 2>&1 | grep -o -m1 -E 'ACCESS_DENIED|^[^ ]+$'
}

hits_of_last_run()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "
        SELECT ProfileEvents['QueryPlanCacheHits']
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND type = 'QueryFinish'
          AND query LIKE '$1%'
        ORDER BY event_time_microseconds DESC
        LIMIT 1"
}

# Warms the query $2 with the grant, repeats it, then revokes the grant and repeats it again.
check()
{
    echo "-- $1"
    $CLICKHOUSE_CLIENT --query "GRANT INTROSPECTION ON *.* TO $user"
    $CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
    run_user "$2"
    run_user "$2"
    echo "-- hits (must be 0): $(hits_of_last_run "$3")"
    $CLICKHOUSE_CLIENT --query "REVOKE INTROSPECTION ON *.* FROM $user"
    run_user "$2"
}

check "arrayReduce" \
    "SELECT /* q1 */ notEmpty(arrayReduce('flameGraph', [[toUInt64(1)]])) FROM t_05843" 'SELECT /* q1 */'

check "arrayReduce with a computed name" \
    "SELECT /* q2 */ notEmpty(arrayReduce(concat('flame', 'Graph'), [[toUInt64(1)]])) FROM t_05843" 'SELECT /* q2 */'

check "arrayReduceInRanges" \
    "SELECT /* q3 */ notEmpty(arrayReduceInRanges('flameGraph', [(1, 1)], [[toUInt64(1)]])) FROM t_05843" 'SELECT /* q3 */'

check "initializeAggregation" \
    "SELECT /* q4 */ notEmpty(initializeAggregation('flameGraph', [toUInt64(1)])) FROM t_05843" 'SELECT /* q4 */'

check "an aggregate function with a combinator" \
    "SELECT /* q5 */ notEmpty(flameGraphIf([id], id > 0)) FROM t_05843" 'SELECT /* q5 */'

$CLICKHOUSE_CLIENT --query "
    DROP TABLE t_05843;
    DROP USER $user;
"
