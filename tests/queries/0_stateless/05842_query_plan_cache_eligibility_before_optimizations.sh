#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# Regression: the query tree optimization passes can remove an expression that makes a plan
# ineligible for the query plan cache. `IfConstantConditionPass` replaces
# `if(currentRequestURL IN (''), id, id + 100)` with the selected branch and drops the folded
# condition, and the query text shows neither a function call (the niladic function is spelled
# without parentheses) nor anything else the early AST check rejects. A plan warmed over the native
# protocol would then be served to an HTTP request, for which the other branch is the right one.
# So the eligibility is decided on the resolved tree, before the optimizations - both for the query
# itself and for the body of an expanded view.
# The plan cache is a single, server-wide cache inspected via `SYSTEM DROP QUERY PLAN CACHE` and
# exact `QueryPlanCacheHits` counts, so the test runs in isolation (see 04489 for the full
# rationale of the tags).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1"

$CLICKHOUSE_CLIENT --query "
    DROP VIEW IF EXISTS v_05842;
    DROP TABLE IF EXISTS t_05842;
    CREATE TABLE t_05842 (id UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO t_05842 VALUES (1), (2), (3);
    CREATE VIEW v_05842 AS SELECT if(currentRequestURL IN (''), id, id + 100) AS x FROM t_05842;
"

run_native()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $SETTINGS --query "$1" 2>&1
}

# Number of plan cache hits recorded for the most recent run of a query starting with $1.
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

# Runs the query $1 over HTTP and prints its result and the number of plan cache hits.
run_http()
{
    local http_query_id="05842_http_${CLICKHOUSE_DATABASE}_${RANDOM}"
    echo "-- HTTP: $(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query_id=${http_query_id}&allow_experimental_query_plan_cache=1&enable_query_plan_cache=1" -d "$1" | tr '\n' ' ')"
    # The `query_log` entry of an HTTP query is written after the response is sent, so wait for it.
    local http_hits=""
    while clickhouse_test_time_left
    do
        $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
        http_hits=$($CLICKHOUSE_CLIENT --query "
            SELECT ProfileEvents['QueryPlanCacheHits']
            FROM system.query_log
            WHERE event_date >= yesterday() AND query_id = '${http_query_id}' AND type = 'QueryFinish'")
        [ -n "$http_hits" ] && break
        sleep 0.1
    done
    echo "-- hits of the HTTP run (must be 0): ${http_hits}"
}

$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"

echo "-- 1. a constant condition removed from the query by the optimizations"
Q1="SELECT /* q1 */ if(currentRequestURL IN (''), id, id + 100) FROM ${CLICKHOUSE_DATABASE}.t_05842 ORDER BY id"
echo "-- native: $(run_native "$Q1" | tr '\n' ' ')"
run_native "$Q1" > /dev/null
echo "-- hits after a native repeat (must be 0): $(hits_of_last_run 'SELECT /* q1 */')"
run_http "$Q1"

echo "-- 2. a constant condition removed from the body of an expanded view"
Q2="SELECT /* q2 */ x FROM ${CLICKHOUSE_DATABASE}.v_05842 ORDER BY x"
echo "-- native: $(run_native "$Q2" | tr '\n' ' ')"
run_native "$Q2" > /dev/null
echo "-- hits after a native repeat (must be 0): $(hits_of_last_run 'SELECT /* q2 */')"
run_http "$Q2"

echo "-- 3. a deterministic constant condition does not prevent caching"
Q3="SELECT /* q3 */ if(1 IN (1), id, id + 100) FROM ${CLICKHOUSE_DATABASE}.t_05842 ORDER BY id"
echo "-- native: $(run_native "$Q3" | tr '\n' ' ')"
run_native "$Q3" > /dev/null
echo "-- hits after a native repeat (must be 1): $(hits_of_last_run 'SELECT /* q3 */')"

$CLICKHOUSE_CLIENT --query "
    DROP VIEW v_05842;
    DROP TABLE t_05842;
"
