#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# Regression: a SQL UDF passed by name to a higher-order function (`arrayExists(f, [id])`,
# `arrayMap(f, xs)`) is an `ASTIdentifier`, not an `ASTFunction`, but the analyzer still inlines its
# body. Such a query, or a query reading a table whose row policy does that, must not be cached:
# otherwise `CREATE OR REPLACE FUNCTION` leaves a hit serving the old body.
# The plan cache is server-wide and inspected via exact `QueryPlanCacheHits` counts, and the test
# creates a global user and global functions, so it runs in isolation (see 04489).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05458_${CLICKHOUSE_DATABASE}"
SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_05458;
    CREATE TABLE t_05458 (id UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO t_05458 VALUES (1), (2);
    CREATE OR REPLACE FUNCTION f_05458 AS x -> x <= 2;

    DROP USER IF EXISTS $user;
    CREATE USER $user;
    REVOKE ALL ON *.* FROM $user;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_05458 TO $user;
    CREATE ROW POLICY p_05458 ON ${CLICKHOUSE_DATABASE}.t_05458 USING arrayExists(f_05458, [id]) TO $user;
"

hits_of_last_run()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "
        SELECT ProfileEvents['QueryPlanCacheHits']
        FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE '$1%'
        ORDER BY event_time_microseconds DESC
        LIMIT 1"
}

echo "-- row policy passing a SQL UDF by name"
POLICY_QUERY="SELECT id FROM ${CLICKHOUSE_DATABASE}.t_05458 ORDER BY id"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
# shellcheck disable=SC2086
$CLICKHOUSE_CLIENT --user="$user" $SETTINGS --query "$POLICY_QUERY"
$CLICKHOUSE_CLIENT --query "CREATE OR REPLACE FUNCTION f_05458 AS x -> x <= 1"
# shellcheck disable=SC2086
$CLICKHOUSE_CLIENT --user="$user" $SETTINGS --query "$POLICY_QUERY"
echo "hits: $(hits_of_last_run 'SELECT id FROM')"

echo "-- query passing a SQL UDF by name"
QUERY="SELECT arrayMap(f_05458, [1, 2, 3])"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
# shellcheck disable=SC2086
$CLICKHOUSE_CLIENT $SETTINGS --query "$QUERY"
$CLICKHOUSE_CLIENT --query "CREATE OR REPLACE FUNCTION f_05458 AS x -> x <= 2"
# shellcheck disable=SC2086
$CLICKHOUSE_CLIENT $SETTINGS --query "$QUERY"
echo "hits: $(hits_of_last_run 'SELECT arrayMap(f_05458')"

$CLICKHOUSE_CLIENT --query "
    DROP ROW POLICY p_05458 ON ${CLICKHOUSE_DATABASE}.t_05458;
    DROP USER $user;
    DROP FUNCTION f_05458;
    DROP TABLE t_05458;
"
