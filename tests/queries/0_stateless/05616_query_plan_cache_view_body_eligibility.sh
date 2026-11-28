#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# The query plan cache must see the views a query reads, however they are expanded:
#   1. With `analyzer_inline_views = 0` a view body is analyzed by its own interpreter while the plan
#      is built, so the eligibility check of the caller's query tree does not see it. A body that
#      folds a non-deterministic function - also spelled without parentheses (`now`) or coming from
#      an `ALIAS` column - must keep the query off the cache.
#   2. With `analyzer_inline_views = 1` the analyzer replaces the view by its body, so the planner
#      never sees the view. A cache hit must still notice a redefinition of the view, a tightened
#      row policy on it and a revoked `SELECT` grant on it.
# The plan cache is a single, server-wide cache inspected via `SYSTEM DROP QUERY PLAN CACHE` and
# exact `QueryPlanCacheHits`, so this test runs in isolation.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1"
USER_NAME="user_05616_${CLICKHOUSE_DATABASE}"
POLICY_NAME="policy_05616_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "
    DROP USER IF EXISTS ${USER_NAME};
    CREATE TABLE t (id UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO t VALUES (1), (2), (3);
    CREATE TABLE t_alias (id UInt64, ts DateTime ALIAS now) ENGINE = MergeTree ORDER BY id;
    INSERT INTO t_alias VALUES (1);
    CREATE VIEW v_now AS SELECT now AS ts;
    CREATE VIEW v_current_timestamp AS SELECT CURRENT_TIMESTAMP AS ts;
    CREATE VIEW v_alias AS SELECT id, ts FROM t_alias;
    CREATE VIEW v_deterministic AS SELECT id + 1 AS x FROM t;
    CREATE VIEW v_inline AS SELECT id + 1 AS x FROM t;
    CREATE USER ${USER_NAME};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t TO ${USER_NAME};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v_inline TO ${USER_NAME};
"

hits_of()
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

$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"

echo "-- 1. non-deterministic view bodies are not cached"
for view in v_now v_current_timestamp v_alias
do
    for _ in 1 2
    do
        # shellcheck disable=SC2086
        $CLICKHOUSE_CLIENT $SETTINGS --analyzer_inline_views 0 --query "SELECT ts > 0 FROM ${view}"
    done
    echo "${view} hits: $(hits_of "SELECT ts > 0 FROM ${view}")"
done

for _ in 1 2
do
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $SETTINGS --analyzer_inline_views 0 --query "SELECT sum(x) FROM v_deterministic"
done
echo "v_deterministic hits: $(hits_of 'SELECT sum(x) FROM v_deterministic')"

run_inline()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $SETTINGS --analyzer_inline_views 1 --user "${USER_NAME}" --query "SELECT sum(x) FROM v_inline" 2>&1 \
        | grep -o -m1 -E 'ACCESS_DENIED|^[0-9]+$'
}

echo "-- 2. dependencies of inlined views"
run_inline
run_inline
echo "warm hits: $(hits_of 'SELECT sum(x) FROM v_inline')"

$CLICKHOUSE_CLIENT --query "CREATE OR REPLACE VIEW v_inline AS SELECT id + 2 AS x FROM t"
echo "after redefinition:"
run_inline
run_inline

$CLICKHOUSE_CLIENT --query "CREATE ROW POLICY ${POLICY_NAME} ON ${CLICKHOUSE_DATABASE}.v_inline FOR SELECT USING x >= 4 TO ${USER_NAME}"
echo "after row policy:"
run_inline
run_inline
$CLICKHOUSE_CLIENT --query "ALTER ROW POLICY ${POLICY_NAME} ON ${CLICKHOUSE_DATABASE}.v_inline FOR SELECT USING x >= 5"
echo "after tightened row policy:"
run_inline
run_inline

$CLICKHOUSE_CLIENT --query "REVOKE SELECT ON ${CLICKHOUSE_DATABASE}.v_inline FROM ${USER_NAME}"
echo "after revoke:"
run_inline

$CLICKHOUSE_CLIENT --query "
    DROP ROW POLICY ${POLICY_NAME} ON ${CLICKHOUSE_DATABASE}.v_inline;
    DROP USER ${USER_NAME};
"
