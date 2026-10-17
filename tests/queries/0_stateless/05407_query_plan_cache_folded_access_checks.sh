#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas, no-fasttest
# The query plan cache is a single, server-wide cache that this test inspects via the global
# `SYSTEM DROP QUERY PLAN CACHE` and exact `QueryPlanCacheHits` counts, and the test creates a global
# user (no-parallel). Whether a second run hits depends on the exact logical plan, which random
# settings perturb. The cache only engages with the analyzer and is incompatible with parallel
# replicas. Introspection functions are not available in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression tests: access checks that happen during analysis of a part of the query that is folded
# into a constant must not be lost on a hit after the privilege is revoked.

user="user_05407_${CLICKHOUSE_DATABASE}"
SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1"

$CLICKHOUSE_CLIENT --query "
    DROP USER IF EXISTS $user;
    CREATE USER $user;
    REVOKE ALL ON *.* FROM $user;
"

run_user()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT --user="$user" $SETTINGS "$@" 2>&1 | grep -o -m1 -E 'ACCESS_DENIED|^[^ ]+$'
}

hits_of_last_run()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
    $CLICKHOUSE_CLIENT --query "
        SELECT ProfileEvents['QueryPlanCacheHits']
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND type = 'QueryFinish'
          AND query = '$1'
        ORDER BY event_time_microseconds DESC
        LIMIT 1"
}

echo "-- a privileged function folded inside an invoker view"
$CLICKHOUSE_CLIENT --allow_introspection_functions=1 --query "
    DROP VIEW IF EXISTS v_demangle;
    CREATE VIEW v_demangle SQL SECURITY INVOKER AS SELECT demangle('_Z3foov') AS s;
    GRANT SELECT ON v_demangle TO $user;
    GRANT demangle ON *.* TO $user;
"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
QUERY="SELECT s FROM v_demangle"
run_user --allow_introspection_functions=1 --query "$QUERY"
run_user --allow_introspection_functions=1 --query "$QUERY"
echo "-- hits (must be 0): $(hits_of_last_run "$QUERY")"
$CLICKHOUSE_CLIENT --query "REVOKE demangle ON *.* FROM $user"
run_user --allow_introspection_functions=1 --query "$QUERY"

echo "-- a Dictionary table read inside a folded scalar subquery"
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_dict_wrapper;
    DROP DICTIONARY IF EXISTS d_05407;
    DROP TABLE IF EXISTS t_dict_source;
    CREATE TABLE t_dict_source (id UInt64, value UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO t_dict_source VALUES (1, 10), (2, 20);
    CREATE DICTIONARY d_05407 (id UInt64, value UInt64) PRIMARY KEY id
        SOURCE(CLICKHOUSE(TABLE 't_dict_source' DB '$CLICKHOUSE_DATABASE')) LAYOUT(FLAT()) LIFETIME(0);
    CREATE TABLE t_dict_wrapper (id UInt64, value UInt64) ENGINE = Dictionary($CLICKHOUSE_DATABASE.d_05407);
    GRANT SELECT ON t_dict_wrapper TO $user;
    GRANT dictGet ON d_05407 TO $user;
"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
QUERY="SELECT (SELECT sum(value) FROM t_dict_wrapper)"
run_user --query_plan_cache_allow_scalar_subqueries=1 --query "$QUERY"
run_user --query_plan_cache_allow_scalar_subqueries=1 --query "$QUERY"
echo "-- hits (must be 0): $(hits_of_last_run "$QUERY")"
$CLICKHOUSE_CLIENT --query "REVOKE dictGet ON d_05407 FROM $user"
run_user --query_plan_cache_allow_scalar_subqueries=1 --query "$QUERY"

echo "-- a Set table on the right side of IN inside a folded scalar subquery"
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_in_scalar;
    DROP TABLE IF EXISTS t_set_scalar;
    CREATE TABLE t_in_scalar (id UInt64) ENGINE = Memory;
    INSERT INTO t_in_scalar VALUES (1), (2), (3);
    CREATE TABLE t_set_scalar (id UInt64) ENGINE = Set;
    INSERT INTO t_set_scalar VALUES (1), (2);
    GRANT SELECT ON t_in_scalar TO $user;
    GRANT SELECT ON t_set_scalar TO $user;
"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
QUERY="SELECT (SELECT count() FROM t_in_scalar WHERE id IN t_set_scalar) AS n"
run_user --query_plan_cache_allow_scalar_subqueries=1 --query "$QUERY"
run_user --query_plan_cache_allow_scalar_subqueries=1 --query "$QUERY"
echo "-- hits (warm): $(hits_of_last_run "$QUERY")"
$CLICKHOUSE_CLIENT --query "REVOKE SELECT ON t_set_scalar FROM $user"
run_user --query_plan_cache_allow_scalar_subqueries=1 --query "$QUERY"

$CLICKHOUSE_CLIENT --query "
    DROP VIEW v_demangle;
    DROP TABLE t_dict_wrapper;
    DROP DICTIONARY d_05407;
    DROP TABLE t_dict_source;
    DROP TABLE t_in_scalar;
    DROP TABLE t_set_scalar;
    DROP USER $user;
"
