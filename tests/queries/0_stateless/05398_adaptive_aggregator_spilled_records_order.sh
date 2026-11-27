#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The merge of the adaptive aggregator adds every thread's staged records in the order the thread staged them: first
# those it wrote to disk, then those it kept in memory. So `groupArray` lists the values of a key that one thread staged
# in the order of that thread's rows, as with the feature off. Each lane of `UNION ALL` is its own thread, and
# `max_threads_min_free_memory_per_thread = 0` keeps both threads whatever the free memory. The tables
# freeze at their first key, so the rows of key 1024 of each lane, almost all of them, are staged, and a spill threshold
# of a megabyte writes most of them to disk while the latest ones stay in memory. The result matches the same query with
# the feature off and no spilling. The thresholds are pinned because the runner randomizes them, and the query runs in
# its own clickhouse-local process, so the counters in `system.events` belong to it alone.
query="SELECT sum(cityHash64(a)) FROM (
    SELECT lane * 10000 + if(number < 1024, number, 1024) AS k, groupArray(number) AS a
    FROM (SELECT toUInt64(0) AS lane, number FROM numbers(100000) UNION ALL SELECT toUInt64(1) AS lane, number FROM numbers(100000))
    GROUP BY k SETTINGS __SETTINGS__)"
off=${query//__SETTINGS__/enable_adaptive_aggregator = 0}
on=${query//__SETTINGS__/enable_adaptive_aggregator = 1, max_bytes_before_external_group_by = 1000000}

$CLICKHOUSE_LOCAL --query "
SET max_threads = 2, max_threads_min_free_memory_per_thread = 0, max_block_size = 1024;
SET adaptive_aggregator_freeze_threshold = 1, adaptive_aggregator_freeze_threshold_bytes = 0, adaptive_aggregator_disable_thaw = 1;
SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 100000000;
SET collect_hash_table_stats_during_aggregation = 0;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

SELECT 'matches the baseline', (${off}) = (${on});
SELECT 'spilled a prefix and kept a tail', spilled > 0 AND spilled < staged
FROM (SELECT
    (SELECT sum(value) FROM system.events WHERE event = 'AdaptiveAggregationSpilledRecords') AS spilled,
    (SELECT sum(value) FROM system.events WHERE event = 'AdaptiveAggregationStagedRecords') AS staged);
"
