#!/usr/bin/env bash
# Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-old-analyzer, no-parallel-replicas
# Regression test: a deserialized `AggregatingStep` spilled into the server-wide temporary-data scope
# instead of the scope of the current query, so on a plan cache hit an external aggregation was not
# subject to `max_temporary_data_on_disk_size_for_query`: the first (planned) query failed at the
# one-byte cap while the identical second query, served from the cache, spilled and succeeded.
# The plan cache is a single, server-wide cache, so the test runs in isolation (see 04489 for the
# full rationale of the tags).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query "SYSTEM DROP QUERY PLAN CACHE"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (k UInt64) ENGINE = Memory;
    INSERT INTO t SELECT number FROM numbers(200000);
"

SETTINGS="
    SET allow_experimental_query_plan_cache = 1, enable_query_plan_cache = 1;
    SET max_threads = 1;
    SET max_bytes_before_external_group_by = 100000, min_bytes_before_external_group_by = 0;
    SET max_bytes_ratio_before_external_group_by = 0;
    SET group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 0;
    SET max_temporary_data_on_disk_size_for_query = 1;
"

for _ in 1 2
do
    $CLICKHOUSE_CLIENT --query "$SETTINGS SELECT k, count() FROM t GROUP BY k FORMAT Null" 2>&1 \
        | grep -o -m1 "TOO_MANY_ROWS_OR_BYTES" || echo "no error"
done

$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
echo "-- both queries hit the temporary data limit, the second one with the cached plan"
$CLICKHOUSE_CLIENT --query "
    SELECT count(), sum(ProfileEvents['QueryPlanCacheHits'])
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'ExceptionWhileProcessing' AND query LIKE 'SELECT k, count() FROM t%'"

$CLICKHOUSE_CLIENT --query "DROP TABLE t"
