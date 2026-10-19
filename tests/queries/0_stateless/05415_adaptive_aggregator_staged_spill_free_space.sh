#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A table of the adaptive aggregator freezes within its first few thousand of the 100000 keys, so the records of the
# other keys are staged, and over the external-aggregation threshold they spill to streams that grow with every spill.
# `count` does not grow, so no table is written before the threads finish, and the staged records are the only data
# that goes to disk until then. A spill creates its streams on a disk with room for the spill together with
# `min_free_disk_space_for_temporary_data`, so an unattainable minimum stops the query at its first spill, before any
# staged record is written. The same query without the minimum shows that it spills staged records. The thresholds are
# pinned because the runner randomizes them, and each run is a clickhouse-local process of its own, so the counters in
# `system.events` belong to it alone.
settings="
    SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 8192;
    SET enable_adaptive_aggregator = 1, adaptive_aggregator_disable_thaw = 1, collect_hash_table_stats_during_aggregation = 0;
    SET adaptive_aggregator_freeze_threshold = 1000, adaptive_aggregator_freeze_threshold_bytes = 0;
    SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 500000000;
    SET max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0;"
query="SELECT count(), sum(c) FROM (SELECT toUInt64(number % 100000) AS k, count() AS c FROM numbers_mt(400000) GROUP BY k)"
spilled="SELECT 'staged records written to disk',
    (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationSpilledRecords') > 0"

$CLICKHOUSE_LOCAL --query "${settings} ${query}; ${spilled};
    SELECT 'frozen tables written to disk',
        (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationFrozenTableSpills');"

echo 'An unattainable minimum of free space'
$CLICKHOUSE_LOCAL --ignore-error --query "${settings} SET min_free_disk_space_for_temporary_data = 1000000000000000000;
    ${query}; ${spilled};" 2>&1 | grep -oE "NOT_ENOUGH_SPACE|staged records written to disk.*" | sort -u
