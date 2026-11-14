#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# Regression test: a constraint is folded into the plan by the constraint-based optimizations, but it is
# not part of the analyzed query tree. A constraint calling a non-deterministic function (`getSetting`)
# was frozen into the cached plan: with `max_threads` ignored by the cache key, the second query reused
# the predicate rewritten under `max_threads = 10` and lost the row `25`. Such a table is not cacheable.
# The plan cache is a single, server-wide cache, so the test runs in isolation (see 04489 for the full
# rationale of the tags).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"

$CLICKHOUSE_CLIENT --query "
    SET allow_experimental_query_plan_cache = 1, enable_query_plan_cache = 1;
    SET convert_query_to_cnf = 1, optimize_using_constraints = 1, max_threads = 10;
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (b UInt64, CONSTRAINT c ASSUME b <= getSetting('max_threads')) ENGINE = Memory;
    INSERT INTO t VALUES (7);
    SELECT b FROM t WHERE b > 20 OR b < 8 ORDER BY b;
    SET max_threads = 30;
    INSERT INTO t VALUES (25);
    SELECT b FROM t WHERE b > 20 OR b < 8 ORDER BY b;
"

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
echo "-- the plan was not cached"
$CLICKHOUSE_CLIENT --query "
    SELECT count(), sum(ProfileEvents['QueryPlanCacheHits'])
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE 'SELECT b FROM t%'"

$CLICKHOUSE_CLIENT --query "DROP TABLE t"
