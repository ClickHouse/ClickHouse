#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A frozen table of the adaptive aggregator stages the arguments of its missed rows from the argument columns that the
# aggregation prepared for the block, which hold a constant argument expanded once. A block of 16384 rows with a
# four-kilobyte constant argument expands to 64 MiB, and each of the two threads holds the copy of its block, so the
# query takes about 128 MiB; another copy per thread for the staging would take it to about 256 MiB. The frozen tables
# hold one key, and one row of each block has the other key, so every block stages a record. The query runs under a
# limit between the two amounts, with the feature off and on, and the adaptive run freezes its tables and stages
# records. The thresholds are pinned because the runner randomizes them, and
# `max_threads_min_free_memory_per_thread = 0` keeps both threads whatever the free memory. The queries run in their
# own clickhouse-local process, so the counters in `system.events` belong to them alone.
query="SELECT count(), sum(length(v)) FROM (
    SELECT toUInt64(number % 16384 = 16383) AS k, argMin(repeat('x', 4096), number) AS v FROM numbers_mt(262144) GROUP BY k)"

$CLICKHOUSE_LOCAL --query "
SET max_threads = 2, max_block_size = 16384, max_threads_min_free_memory_per_thread = 0;
SET adaptive_aggregator_freeze_threshold = 1, adaptive_aggregator_freeze_threshold_bytes = 0, adaptive_aggregator_disable_thaw = 1;
SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 500000000;
SET collect_hash_table_stats_during_aggregation = 0;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
SET max_memory_usage = 167772160;

${query} SETTINGS enable_adaptive_aggregator = 0;
${query} SETTINGS enable_adaptive_aggregator = 1;
SELECT 'tables frozen', (SELECT count() FROM system.events WHERE event = 'AdaptiveAggregationLocalFreezes') > 0;
SELECT 'records staged', (SELECT count() FROM system.events WHERE event = 'AdaptiveAggregationStagedRecords') > 0;
"
