#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# A scalar supplied by the client (`--external --scalar`) is folded into the plan as a constant by
# `__getScalar`, but it is part of neither the plan cache key nor the dependency set. Queries of
# such a session must not use the plan cache, otherwise a hit would return the scalar of the run
# that warmed the entry.
# The plan cache is a single, server-wide cache inspected via `SYSTEM DROP QUERY PLAN CACHE` and
# exact `QueryPlanCacheHits` counts, so the test runs in isolation (see 04489 for the full
# rationale of the tags).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SETTINGS="--allow_experimental_query_plan_cache=1 --enable_query_plan_cache=1"

$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t (k UInt64) ENGINE = MergeTree ORDER BY k;
    INSERT INTO t VALUES (1), (2), (3);
    SYSTEM DROP QUERY PLAN CACHE;
"

QUERY="SELECT __getScalar('foo'), sum(k) FROM t"

run_with_scalar()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $SETTINGS --external --scalar --name foo --format CSV --file <(echo "$1") --types 'String' --query "$QUERY"
}

run_with_scalar one
run_with_scalar two
run_with_scalar three

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
echo "-- plan cache hits and misses (must be 0)"
$CLICKHOUSE_CLIENT --query "
    SELECT sum(ProfileEvents['QueryPlanCacheHits']), sum(ProfileEvents['QueryPlanCacheMisses'])
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND type = 'QueryFinish'
      AND query LIKE 'SELECT \\_\\_getScalar(%'"

$CLICKHOUSE_CLIENT --query "DROP TABLE t"
