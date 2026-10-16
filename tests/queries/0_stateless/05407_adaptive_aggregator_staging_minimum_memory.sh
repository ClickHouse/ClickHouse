#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A frozen table of the adaptive aggregator stages its missed rows in partitions that nest in the 256 buckets of the
# two-level hash tables, and every thread starts each partition of its staging with a first chunk. An
# external-aggregation threshold reduces the partitions so that the first chunks of all threads fit within a quarter
# of it, and a threshold too small even for one partition per bucket reduces the first chunks, down to 128 bytes.
# With an immediate freeze, 64 threads stage every row of 256 keys, and a threshold of one byte spills the records
# after every block. The records of a block reach every group of 16 partitions, and a group's first block takes the
# first chunks of all its partitions: at 128 bytes the query takes about 16 MiB, while the first chunks of 2 KiB that
# a larger threshold affords would take 512 KiB per thread, about 40 MiB together. The query runs under a limit
# between the two amounts, with the feature off and on. The thresholds are pinned because the freeze and the spill
# depend on them, and `max_threads_min_free_memory_per_thread = 0` keeps all threads whatever the free memory. The
# queries run in their own clickhouse-local process, so the counters in `system.events` belong to them alone.
query="SELECT count(), sum(c) FROM (SELECT toUInt64(number % 256) AS k, count() AS c FROM numbers_mt(1048576) GROUP BY k)"

$CLICKHOUSE_LOCAL --query "
SET max_threads = 64, max_threads_min_free_memory_per_thread = 0, max_block_size = 1024;
SET adaptive_aggregator_freeze_threshold = 0, adaptive_aggregator_freeze_threshold_bytes = 0, adaptive_aggregator_disable_thaw = 1;
SET max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0;
SET collect_hash_table_stats_during_aggregation = 0;
SET max_memory_usage = 25165824;

${query} SETTINGS enable_adaptive_aggregator = 0;
${query} SETTINGS enable_adaptive_aggregator = 1;
SELECT 'tables frozen', (SELECT count() FROM system.events WHERE event = 'AdaptiveAggregationLocalFreezes') > 0;
SELECT 'records spilled', (SELECT count() FROM system.events WHERE event = 'AdaptiveAggregationSpilledRecords') > 0;
"
