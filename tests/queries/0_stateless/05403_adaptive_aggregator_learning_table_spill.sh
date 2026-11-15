#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Over the external-aggregation threshold a learning table of the adaptive aggregator is written to disk once its own
# tracked memory reaches the floor, as a frozen table is once its states grow, and its thread learns its keys again
# from empty. The freeze bounds are out of reach, so the tables keep learning, and a threshold of one byte keeps the
# query over it on every block. The result matches the same query with the feature off and no spilling, no table
# freezes, and the tables go to disk. The thresholds are pinned because the runner randomizes them, and the query runs
# in its own clickhouse-local process, so the counters in `system.events` belong to it alone.
query="SELECT count(), sum(c) FROM (
    SELECT toUInt64(number % 200000) AS k, count() AS c FROM numbers_mt(1000000) GROUP BY k SETTINGS __SETTINGS__)"
off=${query//__SETTINGS__/enable_adaptive_aggregator = 0}
on=${query//__SETTINGS__/enable_adaptive_aggregator = 1, max_bytes_before_external_group_by = 1}

$CLICKHOUSE_LOCAL --query "
SET max_threads = 4, max_block_size = 8192;
SET adaptive_aggregator_freeze_threshold = 1000000000, adaptive_aggregator_freeze_threshold_bytes = 0;
SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 100000000;
SET collect_hash_table_stats_during_aggregation = 0;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

SELECT 'matches the baseline', (${off}) = (${on});
SELECT 'tables frozen', (SELECT count() FROM system.events WHERE event = 'AdaptiveAggregationLocalFreezes') > 0;
SELECT 'learning tables written to disk',
    (SELECT count() FROM system.events WHERE event = 'ExternalAggregationWritePart') > 0;
"
