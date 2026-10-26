#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The tracked memory of an adaptive aggregator's table counts only what the table retains: the key and argument columns
# that a block materializes are charged to the aggregator. Both parts read a table whose first 16384 rows carry an
# eight-kilobyte `LowCardinality` argument, which a block of 8192 rows materializes into 64 MiB, while the other rows
# carry an empty one.
#
# In the first part the tables freeze at 128 keys among the wide arguments, and their `uniqExactArray` states keep
# growing in heap memory of their own over the later blocks. Over an external-aggregation threshold of 16 MiB the frozen
# tables are written to disk as they grow, so the query stays within a limit of 192 MiB. In the second part 128 groups
# of `max` states stay far below the byte bound of the freeze, so the tables keep learning.
#
# The thresholds are pinned because the runner randomizes them, and each part runs in its own clickhouse-local process,
# so the counters in `system.events` belong to it alone.
function run_part()
{
    local label=$1 settings=$2 queries=$3
    echo "$label"
    $CLICKHOUSE_LOCAL --query "
    CREATE TABLE t (n UInt64, v LowCardinality(String), a Array(UInt64)) ENGINE = Memory;
    INSERT INTO t SELECT number, toLowCardinality(if(number < 16384, repeat('x', 8192), '')),
        if(number < 16384, range(256), range(number * 256, (number + 1) * 256))
    FROM numbers(65536) SETTINGS max_threads = 1, max_block_size = 8192, max_memory_usage = 0;

    SET max_threads = 2, max_block_size = 8192, max_threads_min_free_memory_per_thread = 0;
    SET collect_hash_table_stats_during_aggregation = 0;
    $settings
    $queries
    "
}

run_part 'Wide arguments at the freeze, then growing heap states' "
    SET adaptive_aggregator_freeze_threshold = 128, adaptive_aggregator_freeze_threshold_bytes = 0, adaptive_aggregator_disable_thaw = 1;
    SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 500000000;
    SET max_bytes_before_external_group_by = 16777216, max_bytes_ratio_before_external_group_by = 0;
    SET max_memory_usage = 201326592;" "
    SELECT count(), sum(u), sum(length(mv)) FROM (
        SELECT toUInt64(n % 128) AS k, uniqExactArray(a) AS u, max(v) AS mv FROM t GROUP BY k SETTINGS enable_adaptive_aggregator = 1);
    SELECT 'frozen tables written to disk', (SELECT count() FROM system.events WHERE event = 'AdaptiveAggregationFrozenTableSpills') > 0;"

run_part 'A few groups with a wide argument' "
    SET adaptive_aggregator_freeze_threshold = 16384, adaptive_aggregator_freeze_threshold_bytes = 4194304;
    SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 500000000;
    SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;" "
    SELECT count(), sum(length(mv)) FROM (SELECT toUInt64(n % 128) AS k, max(v) AS mv FROM t GROUP BY k SETTINGS enable_adaptive_aggregator = 1);
    SELECT 'tables frozen', (SELECT count() FROM system.events WHERE event = 'AdaptiveAggregationLocalFreezes') > 0;"
