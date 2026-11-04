#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# Regression test: a constraint is folded into the plan by the constraint-based optimizations, but it is
# not part of the analyzed query tree. A scalar subquery in a constraint was folded into the cached plan
# with the value it read at warm-up, although `query_plan_cache_allow_scalar_subqueries = 0`; after the
# subquery's table changed, a hit kept rewriting the predicate with the old bound and lost the row `25`.
# Such a table is not cacheable. The plan cache is a single, server-wide cache, so the test runs in
# isolation (see 04489 for the full rationale of the tags).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"

$CLICKHOUSE_CLIENT --query "
    SET allow_experimental_query_plan_cache = 1, enable_query_plan_cache = 1;
    SET query_plan_cache_allow_scalar_subqueries = 0;
    SET convert_query_to_cnf = 1, optimize_using_constraints = 1;
    DROP TABLE IF EXISTS t;
    DROP TABLE IF EXISTS lim;
    CREATE TABLE lim (v UInt64) ENGINE = Memory;
    INSERT INTO lim VALUES (10);
    CREATE TABLE t (b UInt64, CONSTRAINT c ASSUME b <= (SELECT max(v) FROM lim)) ENGINE = Memory;
    INSERT INTO t VALUES (7);
    SELECT t.b FROM t CROSS JOIN lim WHERE t.b > 20 OR t.b < 8 ORDER BY t.b;
    TRUNCATE TABLE lim;
    INSERT INTO lim VALUES (30);
    INSERT INTO t VALUES (25);
    SELECT t.b FROM t CROSS JOIN lim WHERE t.b > 20 OR t.b < 8 ORDER BY t.b;
"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
echo "-- the plan was not cached"
$CLICKHOUSE_CLIENT --query "
    SELECT count(), sum(ProfileEvents['QueryPlanCacheHits'])
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE 'SELECT t.b FROM t%'"

$CLICKHOUSE_CLIENT --query "DROP TABLE t"
$CLICKHOUSE_CLIENT --query "DROP TABLE lim"
