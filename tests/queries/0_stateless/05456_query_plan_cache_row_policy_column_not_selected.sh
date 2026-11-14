#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# Regression test for row policies over columns the query does not select, in a cacheable plan:
# 1. The policy pushed into the read of a `MergeTree` table consumes the column only the policy reads,
#    while the read placeholder of the cached plan still lists it, so materializing the plan failed with
#    `THERE_IS_NO_COLUMN` on the very first cache-enabled execution.
# 2. The columns the policy reads were recorded as columns the user must be granted, so a user with
#    `SELECT` only on an `ALIAS` column got `ACCESS_DENIED` on a cache hit, although the grants had not changed.
# The plan cache is a single, server-wide cache and the test creates a global user, so it runs in
# isolation (see 04489 for the full rationale of the tags).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05456_${CLICKHOUSE_DATABASE}"
settings="allow_experimental_query_plan_cache = 1, enable_query_plan_cache = 1"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t;
    DROP TABLE IF EXISTS t_alias;
    CREATE TABLE t (id UInt32, allowed UInt8) ENGINE = MergeTree ORDER BY id;
    INSERT INTO t VALUES (1, 1), (2, 0), (3, 1);
    CREATE TABLE t_alias (id UInt32, a UInt32 ALIAS id + 1) ENGINE = MergeTree ORDER BY id;
    INSERT INTO t_alias VALUES (1), (2);

    DROP USER IF EXISTS $user;
    CREATE USER $user;
    REVOKE ALL ON *.* FROM $user;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t TO $user;
    GRANT SELECT(a) ON ${CLICKHOUSE_DATABASE}.t_alias TO $user;

    DROP ROW POLICY IF EXISTS p_05456 ON ${CLICKHOUSE_DATABASE}.t;
    DROP ROW POLICY IF EXISTS p_05456_alias ON ${CLICKHOUSE_DATABASE}.t_alias;
    CREATE ROW POLICY p_05456 ON ${CLICKHOUSE_DATABASE}.t USING allowed = 1 TO $user;
    CREATE ROW POLICY p_05456_alias ON ${CLICKHOUSE_DATABASE}.t_alias USING id = 1 TO $user;
"

$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"

# The first run of each query builds and stores the plan, the second one is served from the cache.
echo "-- the policy reads a column the query does not select"
for _ in 1 2
do
    $CLICKHOUSE_CLIENT --user="$user" --query "SELECT id FROM ${CLICKHOUSE_DATABASE}.t ORDER BY id SETTINGS $settings" 2>&1
done

echo "-- the policy reads the column behind an ALIAS column, granted alone"
for _ in 1 2
do
    $CLICKHOUSE_CLIENT --user="$user" --query "SELECT a FROM ${CLICKHOUSE_DATABASE}.t_alias SETTINGS $settings" 2>&1
done

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
echo "-- the plans were cached"
$CLICKHOUSE_CLIENT --query "
    SELECT sum(ProfileEvents['QueryPlanCacheHits'])
    FROM system.query_log
    WHERE current_database = currentDatabase() AND user = '$user' AND type = 'QueryFinish'"

$CLICKHOUSE_CLIENT --query "
    DROP ROW POLICY IF EXISTS p_05456 ON ${CLICKHOUSE_DATABASE}.t;
    DROP ROW POLICY IF EXISTS p_05456_alias ON ${CLICKHOUSE_DATABASE}.t_alias;
    DROP USER IF EXISTS $user;
    DROP TABLE t;
    DROP TABLE t_alias;
"
