#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# Regression test: a cacheable logical plan must keep the row policy a boundary that user expressions
# cannot cross. A regular read of a `MergeTree` table applies the policy inside the storage. When the
# cacheable plan applied it as an explicit filter step, `mergeFilters` merged it with the user's
# `WHERE` into one condition, so `throwIf` ran on the row the policy hides and revealed it, both on
# the initial cache-enabled execution and on hits.
# The plan cache is a single, server-wide cache and the test creates a global user, so it runs in
# isolation (see 04489 for the full rationale of the tags).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05452_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (id UInt32, allowed UInt8) ENGINE = MergeTree ORDER BY id;
    INSERT INTO t VALUES (1, 1), (2, 0);

    DROP USER IF EXISTS $user;
    CREATE USER $user;
    REVOKE ALL ON *.* FROM $user;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t TO $user;

    DROP ROW POLICY IF EXISTS p_05452 ON ${CLICKHOUSE_DATABASE}.t;
    CREATE ROW POLICY p_05452 ON ${CLICKHOUSE_DATABASE}.t USING allowed TO $user;
"

$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"

# The first run builds and stores the plan, the second one is served from the cache.
for _ in 1 2
do
    $CLICKHOUSE_CLIENT --user="$user" --query "
        SELECT id FROM ${CLICKHOUSE_DATABASE}.t WHERE throwIf(allowed = 0) = 0
        SETTINGS allow_experimental_query_plan_cache = 1, enable_query_plan_cache = 1,
            query_plan_enable_optimizations = 1, query_plan_merge_filters = 1, query_plan_filter_push_down = 0,
            optimize_move_to_prewhere = 0, short_circuit_function_evaluation = 'disable'" 2>&1
done

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
echo "-- the plan was cached"
$CLICKHOUSE_CLIENT --query "
    SELECT sum(ProfileEvents['QueryPlanCacheHits'])
    FROM system.query_log
    WHERE current_database = currentDatabase() AND user = '$user' AND type = 'QueryFinish'"

$CLICKHOUSE_CLIENT --query "
    DROP ROW POLICY IF EXISTS p_05452 ON ${CLICKHOUSE_DATABASE}.t;
    DROP USER IF EXISTS $user;
    DROP TABLE t;
"
