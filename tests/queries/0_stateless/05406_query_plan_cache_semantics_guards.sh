#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# The query plan cache is a single, server-wide cache that this test inspects via the global
# `SYSTEM DROP QUERY PLAN CACHE` and exact `QueryPlanCacheHits` counts (no-parallel). Whether a
# second run hits depends on the exact logical plan, which random settings perturb. The cache only
# engages with the analyzer and is incompatible with parallel replicas.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Regression tests: enabling the query plan cache must not change the result of a query, neither on
# the first (cold) execution nor on a hit.

user="user_05406_${CLICKHOUSE_DATABASE}"
SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1"

$CLICKHOUSE_CLIENT --query "
    DROP USER IF EXISTS $user;
    CREATE USER $user;
    REVOKE ALL ON *.* FROM $user;
"

run()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $SETTINGS "$@" 2>&1
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

echo "-- a row policy is applied before an explicit PREWHERE"
$CLICKHOUSE_CLIENT --query "
    DROP ROW POLICY IF EXISTS p_05406 ON t_prewhere;
    DROP TABLE IF EXISTS t_prewhere;
    CREATE TABLE t_prewhere (id UInt32) ENGINE = MergeTree ORDER BY id;
    INSERT INTO t_prewhere VALUES (1), (2);
    CREATE ROW POLICY p_05406 ON t_prewhere USING id = 1 TO $user;
    GRANT SELECT ON t_prewhere TO $user;
"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
QUERY="SELECT id FROM t_prewhere PREWHERE throwIf(id = 2) = 0 SETTINGS query_plan_enable_optimizations = 0"
run --user="$user" --query "$QUERY"
run --user="$user" --query "$QUERY"

echo "-- output_format settings read by SQL functions are part of the cache key"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
QUERY="SELECT toJSONString(toUInt64(42))"
run --output_format_json_quote_64bit_integers=0 --query "$QUERY"
run --output_format_json_quote_64bit_integers=0 --query "$QUERY"
run --output_format_json_quote_64bit_integers=1 --query "$QUERY"

echo "-- the SETTINGS clause of a view body is not lost"
$CLICKHOUSE_CLIENT --query "
    DROP VIEW IF EXISTS v_final;
    DROP TABLE IF EXISTS t_final;
    CREATE TABLE t_final (id UInt32, p UInt8, ver UInt32) ENGINE = ReplacingMergeTree(ver) PARTITION BY p ORDER BY id;
    INSERT INTO t_final VALUES (1, 0, 1), (1, 1, 2);
    CREATE VIEW v_final AS SELECT id FROM t_final FINAL SETTINGS do_not_merge_across_partitions_select_final = 1;
"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
QUERY="SELECT id FROM v_final ORDER BY id"
run --do_not_merge_across_partitions_select_final=0 --query "$QUERY"
run --do_not_merge_across_partitions_select_final=0 --query "$QUERY"
echo "-- hits: $(hits_of_last_run "$QUERY")"

echo "-- the SETTINGS clause of a subquery is not lost"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
QUERY="SELECT id FROM (SELECT id FROM t_final FINAL SETTINGS do_not_merge_across_partitions_select_final = 1) ORDER BY id"
run --do_not_merge_across_partitions_select_final=0 --query "$QUERY"
run --do_not_merge_across_partitions_select_final=0 --query "$QUERY"
echo "-- hits: $(hits_of_last_run "$QUERY")"

echo "-- a temporary table shadows a permanent table of the same name"
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_shadow;
    CREATE TABLE t_shadow (x UInt8) ENGINE = MergeTree ORDER BY x;
    INSERT INTO t_shadow VALUES (1);
"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
QUERY="SELECT x FROM t_shadow"
run --query "$QUERY"
run --query "$QUERY"
echo "-- hits (warm): $(hits_of_last_run "$QUERY")"
run --query "
    CREATE TEMPORARY TABLE t_shadow (x UInt8);
    INSERT INTO t_shadow VALUES (2);
    $QUERY;
"

echo "-- a join that requires the direct join algorithm"
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_left;
    DROP TABLE IF EXISTS t_right;
    CREATE TABLE t_left (id UInt64) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE t_right (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO t_left VALUES (1), (2);
    INSERT INTO t_right VALUES (1, 'a');
"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
QUERY="SELECT l.id, r.val FROM t_left AS l ALL LEFT JOIN t_right AS r ON l.id = r.id ORDER BY l.id SETTINGS join_algorithm = 'direct'"
run --query "$QUERY"
run --query "$QUERY"

echo "-- an ALTER of an ALIAS column whose expressions differ only in masked text invalidates the plan"
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_masked;
    CREATE TABLE t_masked (id UInt8, a String ALIAS 'TOPSECRET1TOPSECRET') ENGINE = Memory;
    INSERT INTO t_masked (id) VALUES (1);
"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"
QUERY="SELECT a FROM t_masked"
run --query "$QUERY"
run --query "$QUERY"
echo "-- hits (warm): $(hits_of_last_run "$QUERY")"
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_masked MODIFY COLUMN a String ALIAS 'TOPSECRET2TOPSECRET'"
run --query "$QUERY"
echo "-- hits after ALTER (must be 0): $(hits_of_last_run "$QUERY")"

$CLICKHOUSE_CLIENT --query "
    DROP ROW POLICY p_05406 ON t_prewhere;
    DROP TABLE t_prewhere;
    DROP VIEW v_final;
    DROP TABLE t_final;
    DROP TABLE t_shadow;
    DROP TABLE t_left;
    DROP TABLE t_right;
    DROP TABLE t_masked;
    DROP USER $user;
"
