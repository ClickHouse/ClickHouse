#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A frozen table of the adaptive aggregator stages the keys of its missed rows. A key with an `Array` is serialized row
# by row on every architecture, so the batch of misses keeps the bytes of the missed keys for the staging, and the
# kernel stages the batch whenever they reach a bound, in the middle of the block if need be. Each of the two streams
# reads a table of its own: one block of 8192 rows with keys of about 4 KiB. Its table freezes with the 7 keys of the
# first rows, so the other half of the rows miss and stage 17 MiB of records. The tables hold the keys, so the memory
# of the query is that of the aggregation: the records of both streams and the spill streams take about 40 MiB, under
# the limit of 64 MiB. Keeping the bytes of all the missed keys of a block would add a buffer of 32 MiB per stream,
# which the spill does not free. The shapes cover the probe loops of the keys alone, of a lone `count` and of aggregates
# with arguments. Each compares its result with the feature off and reports whether records were staged.
# `max_threads_min_free_memory_per_thread = 0` keeps the two threads whatever the free memory. The queries run in their
# own clickhouse-local process, so the counters in `system.events` belong to them alone.
function run_shape()
{
    local label=$1 query=$2
    echo "$label"
    $CLICKHOUSE_LOCAL --query "
    CREATE TABLE keys_a (s String, k Array(UInt64), n UInt64) ENGINE = Memory;
    CREATE TABLE keys_b (s String, k Array(UInt64), n UInt64) ENGINE = Memory;
    INSERT INTO keys_a SELECT concat(repeat('x', 4096), toString(intDiv(number, 4096) % 2)), [number % 7], number
        FROM numbers(8192);
    INSERT INTO keys_b SELECT concat(repeat('x', 4096), toString(intDiv(number, 4096) % 2)), [number % 7], number
        FROM numbers(8192, 8192);
    SET max_threads = 2, max_threads_min_free_memory_per_thread = 0, max_memory_usage = 67108864;
    SET adaptive_aggregator_freeze_threshold = 1, adaptive_aggregator_freeze_threshold_bytes = 0, adaptive_aggregator_disable_thaw = 1;
    SET collect_hash_table_stats_during_aggregation = 0;
    SET max_bytes_before_external_group_by = 1048576, max_bytes_ratio_before_external_group_by = 0;
    SELECT 'matches the baseline',
        (${query//__SETTINGS__/enable_adaptive_aggregator = 0}) = (${query//__SETTINGS__/enable_adaptive_aggregator = 1});
    SELECT 'records staged',
        (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationStagedRecords') > 0;
    "
}

input='SELECT * FROM keys_a UNION ALL SELECT * FROM keys_b'

run_shape 'Keys without aggregates' "SELECT count(), sum(cityHash64(s, k)) FROM (
    SELECT s, k FROM (${input}) GROUP BY s, k SETTINGS __SETTINGS__)"

run_shape 'Keys with a count' "SELECT count(), sum(cityHash64(s, k, c)) FROM (
    SELECT s, k, count() AS c FROM (${input}) GROUP BY s, k SETTINGS __SETTINGS__)"

run_shape 'Keys with arguments' "SELECT count(), sum(cityHash64(s, k, c, t)) FROM (
    SELECT s, k, count() AS c, sum(n) AS t FROM (${input}) GROUP BY s, k SETTINGS __SETTINGS__)"
