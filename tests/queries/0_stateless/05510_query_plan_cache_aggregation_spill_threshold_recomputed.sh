#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# Regression test: `max_bytes_ratio_before_external_group_by` is turned into an absolute spill threshold
# from the memory available when the plan is built. A plan cache hit replayed the threshold computed for
# the warm-up query instead of computing it from the memory available now. The computation logs the
# line checked below, so it must happen both on the miss and on the hit.
# The plan cache is a single, server-wide cache, so the test runs in isolation (see 04489 for the
# full rationale of the tags).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (k UInt64) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO t SELECT number FROM numbers(1000);
"

for _ in 1 2
do
    $CLICKHOUSE_CLIENT --send_logs_level=trace --query "
        SELECT k % 10 AS g, count() FROM t GROUP BY g FORMAT Null
        SETTINGS allow_experimental_query_plan_cache = 1, enable_query_plan_cache = 1,
            max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0.5" 2>&1 \
        | grep -c "Adjusting memory limit before external aggregation"
done

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
echo "-- the second query was served from the cache"
$CLICKHOUSE_CLIENT --query "
    SELECT count(), sum(ProfileEvents['QueryPlanCacheHits'])
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query LIKE '%SELECT k % 10 AS g, count() FROM t GROUP BY g%' AND query NOT LIKE '%system.query_log%'"

$CLICKHOUSE_CLIENT --query "DROP TABLE t"
