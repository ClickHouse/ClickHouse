#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The staged spill of the adaptive aggregator writes at most one temporary file per producer, whatever the number of
# partitions: one per bucket in the first and third shapes, two per bucket in the second, whose threshold leaves room
# for them. The third spills about a hundred blocks per partition, more than a partition indexes in memory.
# Each shape compares its result with the same query with the feature off and no spilling. The thresholds are pinned
# because the runner randomizes them, and each shape runs in its own clickhouse-local process, so the counters in
# `system.events` belong to it alone.
function run_shape()
{
    local threshold=$1
    local query=$2
    local off=${query//__SETTINGS__/enable_adaptive_aggregator = 0, max_bytes_before_external_group_by = 0}
    local on=${query//__SETTINGS__/enable_adaptive_aggregator = 1, max_bytes_before_external_group_by = ${threshold}}
    $CLICKHOUSE_LOCAL --query "
    SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 8192;
    SET adaptive_aggregator_freeze_threshold = 1000, adaptive_aggregator_freeze_threshold_bytes = 0, adaptive_aggregator_disable_thaw = 1;
    SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 500000000;
    SET collect_hash_table_stats_during_aggregation = 0, max_bytes_ratio_before_external_group_by = 0, min_bytes_before_external_group_by = 0;

    SELECT 'matches the baseline', (${off}) = (${on});
    SELECT 'staged records spilled', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationSpills') > 0;
    SELECT 'at most one file per producer',
        (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'ExternalAggregationWritePart') BETWEEN 1 AND 4;
    "
}

echo 'One partition per bucket'
run_shape 1 "SELECT count(), sum(c), sum(cityHash64(k, c)) FROM (
    SELECT number % 100000 AS k, count() AS c FROM numbers_mt(400000) GROUP BY k SETTINGS __SETTINGS__)"

echo 'Two partitions per bucket'
run_shape 16777216 "SELECT count(), sum(c), sum(cityHash64(k, c)) FROM (
    SELECT number % 2000000 AS k, count() AS c FROM numbers_mt(4000000) GROUP BY k SETTINGS __SETTINGS__)"

echo 'More blocks per partition than indexed in memory'
run_shape 1 "SELECT count(), sum(c), sum(cityHash64(k, c)) FROM (
    SELECT number % 2000000 AS k, count() AS c FROM numbers_mt(16000000) GROUP BY k SETTINGS __SETTINGS__)"

# As in 05398, each lane of `UNION ALL` is its own thread, whose table freezes at its first key, so it stages nearly all
# of its rows of key 512. They spill on every block, about five thousand blocks of one partition, more than two levels
# of the index hold, and `groupArray` lists them in the order of the lane's rows only if the blocks are read back in the
# order they were written.
echo 'Order of more blocks of a key than indexed in memory'
query="SELECT sum(cityHash64(a)) FROM (
    SELECT lane * 10000000 + if(number < 512, number, 512) AS k, groupArray(number) AS a
    FROM (SELECT toUInt64(0) AS lane, number FROM numbers(2500000) UNION ALL SELECT toUInt64(1) AS lane, number FROM numbers(2500000))
    GROUP BY k SETTINGS __SETTINGS__)"
off=${query//__SETTINGS__/enable_adaptive_aggregator = 0, max_bytes_before_external_group_by = 0}
on=${query//__SETTINGS__/enable_adaptive_aggregator = 1, max_bytes_before_external_group_by = 1}
$CLICKHOUSE_LOCAL --query "
SET max_threads = 2, max_threads_min_free_memory_per_thread = 0, max_block_size = 512;
SET adaptive_aggregator_freeze_threshold = 1, adaptive_aggregator_freeze_threshold_bytes = 0, adaptive_aggregator_disable_thaw = 1;
SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 100000000;
SET collect_hash_table_stats_during_aggregation = 0, max_bytes_ratio_before_external_group_by = 0, min_bytes_before_external_group_by = 0;

SELECT 'matches the baseline', (${off}) = (${on});
SELECT 'staged records spilled', (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationSpills') > 0;
"
