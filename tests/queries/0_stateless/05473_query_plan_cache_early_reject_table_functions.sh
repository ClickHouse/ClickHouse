#!/usr/bin/env bash
# Tags: no-random-settings, no-old-analyzer, no-parallel-replicas
# A table function read and a reused `MATERIALIZED` CTE (read from a temporary table) can never be
# stored in the query plan cache. They must be rejected by the check of the analyzed query tree,
# before the cacheable logical plan is built, instead of after it (which would pay for planning
# twice on every execution), and still return correct results.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT_DEBUG=${CLICKHOUSE_CLIENT/--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}/--send_logs_level=debug}
SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1 --enable_materialized_cte=1"

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t SELECT number FROM numbers(10);
"

# Prints the result and whether the plan was rejected by the early check of the analyzed query tree
# (1) or by the dependency collection after planning (2).
run()
{
    # shellcheck disable=SC2086
    output=$($CLICKHOUSE_CLIENT_DEBUG $SETTINGS --query "$1" 2>&1)
    echo "$output" | grep -v '<'
    echo "early: $(echo "$output" | grep -c 'Not caching plan: query uses non-deterministic functions, scalar subqueries')"
    echo "late: $(echo "$output" | grep -c 'Not caching plan: query references an unsupported dependency')"
}

echo "-- table function"
run "SELECT count() FROM format(JSONEachRow, '{\"a\":1}\n{\"a\":2}')"

echo "-- reused MATERIALIZED CTE"
run "WITH c AS MATERIALIZED (SELECT x FROM t) SELECT count() FROM c AS a, c AS b"

echo "-- table function in an IN subquery"
run "SELECT count() FROM t WHERE x IN (SELECT a FROM format(JSONEachRow, '{\"a\":1}\n{\"a\":2}'))"
