#!/usr/bin/env bash
# Tags: no-asan, no-tsan, no-msan
# The memory limit is tuned to the allocations of a build without a sanitizer.

# A deferred build allocates its maps only when it inserts the stored blocks. The memory spill scheduler must see these
# bytes coming, so that it hands the build to `GraceHashJoin` before the inserts. Whether the query then fits under the
# limit is up to `GraceHashJoin`, so only the hand-over is checked.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE probe (k String) ENGINE = Memory;
    CREATE TABLE build (k String, x String) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO probe SELECT toString(number * 7) FROM numbers(1000);
    INSERT INTO build SELECT toString(number), toString(number % 10) FROM numbers(2000000);
"

# A serial map for 2M string keys needs more than the 100 MB limit, the stored blocks less. The join's own spill
# threshold is out of reach, so only the scheduler can switch it.
settings="join_algorithm = 'hash', max_threads = 1, enable_hash_join_deferred_build = 1, parallel_hash_join_threshold = 1000000000,
    collect_hash_table_stats_during_joins = 0, use_statistics = 0, query_plan_join_swap_table = 'false', enable_parallel_replicas = 0,
    max_bytes_before_external_join = '100Gi', max_bytes_ratio_before_external_join = 0, grace_hash_join_initial_buckets = 8,
    max_bytes_in_join = 0, max_memory_usage = 100000000"

for scheduler in 0 1; do
    $CLICKHOUSE_CLIENT --log_comment "05417_scheduler_$scheduler" -q "
        SELECT l.k, r.x FROM probe AS l LEFT JOIN build AS r ON l.k = r.k
        SETTINGS $settings, enable_adaptive_memory_spill_scheduler = $scheduler
        FORMAT Null" 2>/dev/null
done

$CLICKHOUSE_CLIENT -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT log_comment, ProfileEvents['HashJoinDeferredBuilds'], ProfileEvents['ExternalJoinWritePart'] > 0
    FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment LIKE '05417_scheduler_%' AND type != 'QueryStart'
    ORDER BY log_comment;
"
