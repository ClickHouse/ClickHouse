#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A frozen table of the adaptive aggregator stages the records of its missed rows. A `FixedString` argument is a
# fixed-size field of the record, however wide, so a fixed-width key with such an argument makes records of one size,
# here of 64 KiB. Such a record is written in its partition directly: a scratch buffer for a batch of 1024 of them would
# take 64 MiB, whatever the number of the records staged. The frozen tables hold one key, and one row of each block of 64
# rows has the other key, so every block stages one record. The argument column of a block takes 4 MiB, and the 64
# staged records together take another 4 MiB, so the query takes far less than its limit of 48 MiB with the feature off
# and on, while one batch buffer would exceed the limit. The adaptive run freezes its tables and stages records. The
# thresholds are pinned because the runner randomizes them, and `max_threads_min_free_memory_per_thread = 0` keeps
# both threads whatever the free memory. The queries run in their own clickhouse-local process, so the counters in
# `system.events` belong to them alone.
query="SELECT count(), sum(length(v)) FROM (
    SELECT toUInt64(number % 64 = 63) AS k, any(toFixedString(repeat('x', 65536), 65536)) AS v FROM numbers_mt(4096) GROUP BY k)"

$CLICKHOUSE_LOCAL --query "
SET max_threads = 2, max_block_size = 64, max_threads_min_free_memory_per_thread = 0;
SET adaptive_aggregator_freeze_threshold = 1, adaptive_aggregator_freeze_threshold_bytes = 0, adaptive_aggregator_disable_thaw = 1;
SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 500000000;
SET collect_hash_table_stats_during_aggregation = 0;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
SET max_memory_usage = 50331648;

${query} SETTINGS enable_adaptive_aggregator = 0;
${query} SETTINGS enable_adaptive_aggregator = 1;
SELECT 'tables frozen', (SELECT count() FROM system.events WHERE event = 'AdaptiveAggregationLocalFreezes') > 0;
SELECT 'records staged', (SELECT count() FROM system.events WHERE event = 'AdaptiveAggregationStagedRecords') > 0;
"
