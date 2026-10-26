#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# The query plan cache is a single, server-wide cache that this test inspects via the global
# `SYSTEM DROP QUERY PLAN CACHE` and exact `QueryPlanCacheHits` counts (no-parallel). The cache only
# engages with the analyzer and is incompatible with parallel replicas.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Settings ignored by the query plan cache are stripped from the cache key only in the `SETTINGS`
# clause that is applied to the query context. A `SETTINGS` clause of a subquery is scoped to that
# subquery, which a cached plan does not reproduce, so it must stay in the key: a query with such a
# clause must not hit the plan cached for the same query without it.

SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1"

run()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $SETTINGS --query "$1"
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

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_inner_settings;
    CREATE TABLE t_inner_settings (id UInt32) ENGINE = MergeTree ORDER BY id;
    INSERT INTO t_inner_settings VALUES (1), (2);
"
$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"

QUERY="SELECT count() FROM (SELECT id FROM t_inner_settings)"
run "$QUERY"
run "$QUERY"
echo "-- warm: $(hits_of_last_run "$QUERY")"

echo "-- an ignored setting in the top-level SETTINGS clause shares the cached plan"
QUERY="SELECT count() FROM (SELECT id FROM t_inner_settings) SETTINGS max_threads = 1"
run "$QUERY"
echo "-- hits: $(hits_of_last_run "$QUERY")"

echo "-- an ignored setting in the SETTINGS clause of a subquery does not"
QUERY="SELECT count() FROM (SELECT id FROM t_inner_settings SETTINGS max_threads = 1)"
run "$QUERY"
run "$QUERY"
echo "-- hits: $(hits_of_last_run "$QUERY")"

echo "-- nor in the SETTINGS clause of a non-last SELECT of a UNION"
QUERY="SELECT count() FROM t_inner_settings UNION ALL SELECT count() FROM t_inner_settings"
run "$QUERY"
run "$QUERY"
echo "-- warm: $(hits_of_last_run "$QUERY")"
QUERY="SELECT count() FROM t_inner_settings SETTINGS max_threads = 1 UNION ALL SELECT count() FROM t_inner_settings"
run "$QUERY"
run "$QUERY"
echo "-- hits: $(hits_of_last_run "$QUERY")"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_inner_settings"
