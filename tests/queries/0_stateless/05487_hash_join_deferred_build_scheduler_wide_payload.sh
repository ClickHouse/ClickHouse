#!/usr/bin/env bash
# Tags: no-asan, no-tsan, no-msan
# The memory limit is tuned to the allocations of a build without a sanitizer.

# When the memory spill scheduler hands a join to `GraceHashJoin`, the stored right blocks stay alive while they are
# converted. So a deferred build must reserve their bytes, as the streaming build does, and not only the bytes its maps
# will add. With wide rows the maps are small next to the stored blocks. Reserving only them switches the deferred build
# too late, and the query runs out of memory where the streaming build succeeds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# 500k right rows of 400 bytes take about 210 MB, and the join does not fit under the 250 MB limit. The join's own spill
# threshold is out of reach, so only the scheduler can switch it. With one initial bucket, `GraceHashJoin` keeps half of
# the stored blocks in memory after the switch. With more, a late switch can still fit. Both sides are read from
# `numbers`, so no MergeTree setting changes the memory the join takes.
settings="join_algorithm = 'hash', hash_join_layout = 'serial', max_threads = 1, max_block_size = 8192,
    collect_hash_table_stats_during_joins = 0, query_plan_join_swap_table = 'false', query_plan_optimize_join_order_randomize = 0,
    enable_parallel_replicas = 0, max_bytes_before_external_join = '100Gi', max_bytes_ratio_before_external_join = 0,
    max_bytes_in_join = 0, grace_hash_join_initial_buckets = 1, enable_adaptive_memory_spill_scheduler = 1,
    max_memory_usage = 250000000"

for deferred in 0 1; do
    $CLICKHOUSE_CLIENT --log_comment "05487_deferred_$deferred" -q "
        SELECT count(), sum(cityHash64(r.s))
        FROM (SELECT number * 7919 * 13 % 1000000007 AS k FROM numbers(1000)) AS l
        LEFT JOIN (SELECT number * 7919 % 1000000007 AS k, repeat(toString(number % 10), 400) AS s FROM numbers(500000)) AS r
        ON l.k = r.k
        SETTINGS $settings, enable_hash_join_deferred_build = $deferred
        FORMAT Null" 2>/dev/null
done

# Both builds switch to `GraceHashJoin`, write buckets to disk and succeed.
$CLICKHOUSE_CLIENT -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT log_comment, exception_code, ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'], ProfileEvents['ExternalJoinWritePart'] > 0
    FROM system.query_log
    WHERE current_database = currentDatabase() AND log_comment LIKE '05487_deferred_%' AND type != 'QueryStart'
    ORDER BY log_comment;
"
