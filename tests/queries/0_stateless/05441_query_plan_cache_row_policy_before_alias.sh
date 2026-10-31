#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# Regression test: a cacheable logical plan applies the row policy as an explicit filter step, and it
# must run before the step computing `ALIAS` columns. A regular read applies the policy inside the
# storage, so an `ALIAS` expression never sees the rows the policy excludes; with the policy filter
# after the `ALIAS` step, `a ALIAS throwIf(id = 2)` threw for the hidden row and revealed it, both on
# the initial cache-enabled execution and on hits.
# The plan cache is a single, server-wide cache and the test creates a global user, so it runs in
# isolation (see 04489 for the full rationale of the tags).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05441_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t_merge_tree;
    DROP TABLE IF EXISTS t_memory;
    CREATE TABLE t_merge_tree (id UInt32, a UInt8 ALIAS throwIf(id = 2)) ENGINE = MergeTree ORDER BY id;
    CREATE TABLE t_memory (id UInt32, a UInt8 ALIAS throwIf(id = 2)) ENGINE = Memory;
    INSERT INTO t_merge_tree VALUES (1), (2);
    INSERT INTO t_memory VALUES (1), (2);

    DROP USER IF EXISTS $user;
    CREATE USER $user;
    REVOKE ALL ON *.* FROM $user;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_merge_tree TO $user;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_memory TO $user;

    DROP ROW POLICY IF EXISTS p_05441_merge_tree ON ${CLICKHOUSE_DATABASE}.t_merge_tree;
    DROP ROW POLICY IF EXISTS p_05441_memory ON ${CLICKHOUSE_DATABASE}.t_memory;
    CREATE ROW POLICY p_05441_merge_tree ON ${CLICKHOUSE_DATABASE}.t_merge_tree USING id = 1 TO $user;
    CREATE ROW POLICY p_05441_memory ON ${CLICKHOUSE_DATABASE}.t_memory USING id = 1 TO $user;
"

$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"

for table in t_merge_tree t_memory
do
    for optimizations in 0 1
    do
        echo "-- $table, query_plan_enable_optimizations = $optimizations"
        # The first run builds and stores the plan, the second one is served from the cache.
        for _ in 1 2
        do
            $CLICKHOUSE_CLIENT --user="$user" --query "
                SELECT id, a FROM ${CLICKHOUSE_DATABASE}.$table
                SETTINGS allow_experimental_query_plan_cache = 1, enable_query_plan_cache = 1,
                    query_plan_enable_optimizations = $optimizations" 2>&1
        done
    done
done

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
echo "-- the plans were cached"
$CLICKHOUSE_CLIENT --query "
    SELECT sum(ProfileEvents['QueryPlanCacheHits'])
    FROM system.query_log
    WHERE current_database = currentDatabase() AND user = '$user' AND type = 'QueryFinish'"

$CLICKHOUSE_CLIENT --query "
    DROP ROW POLICY IF EXISTS p_05441_merge_tree ON ${CLICKHOUSE_DATABASE}.t_merge_tree;
    DROP ROW POLICY IF EXISTS p_05441_memory ON ${CLICKHOUSE_DATABASE}.t_memory;
    DROP USER IF EXISTS $user;
    DROP TABLE t_merge_tree;
    DROP TABLE t_memory;
"
