#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The staged records that the adaptive aggregator writes to disk keep the free space that
# `min_free_disk_space_for_temporary_data` asks for, as the temporary parts of an ordinary spill do. The tables of a
# unique-key `count` freeze at their first key and may not thaw, so under a spill threshold of one byte the producers
# write their staged records to disk from the first blocks on. With an unattainable minimum the query fails with
# `NOT_ENOUGH_SPACE` and writes none of them. Each query runs in its own clickhouse-local process, so the counters in
# `system.events` belong to it alone.
settings="
SET max_threads = 2, max_threads_min_free_memory_per_thread = 0, max_block_size = 1024;
SET enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 1, adaptive_aggregator_freeze_threshold_bytes = 0, adaptive_aggregator_disable_thaw = 1;
SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 100000000;
SET collect_hash_table_stats_during_aggregation = 0;
SET max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0;"
query="SELECT count(), sum(c) FROM (SELECT number AS k, count() AS c FROM numbers_mt(100000) GROUP BY k)"

$CLICKHOUSE_LOCAL --query "
${settings}
${query};
SELECT 'spilled staged records', sum(value) > 0 FROM system.events WHERE event = 'AdaptiveAggregationSpilledRecords';
"

$CLICKHOUSE_LOCAL --ignore-error --query "
${settings}
SET min_free_disk_space_for_temporary_data = 1000000000000000000;
${query};
SELECT 'spilled no staged records', sum(value) = 0 FROM system.events WHERE event = 'AdaptiveAggregationSpilledRecords';
" 2>&1 | grep -o -E 'NOT_ENOUGH_SPACE|spilled no staged records.*' | sort -u
