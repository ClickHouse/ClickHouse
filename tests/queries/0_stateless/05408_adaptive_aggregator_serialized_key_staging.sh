#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A frozen table of the adaptive aggregator stages the keys of its missed rows. The serialized methods build a key by
# serializing the row's key columns. When they serialize row by row, the batch of misses keeps the bytes of every
# missed key, so the staging copies them instead of serializing the row again. A `Dynamic` key is always serialized row
# by row, and the shapes group it without aggregates, with a lone `count`, and with arguments, among them a `String`
# that the records stage at its variable size. A string of about 150 bytes with a number is serialized row by row on
# x86, where serializing the keys of a block at once needs keys below 128 bytes on average, and at once on ARM. The
# tables freeze at 1000 keys, so most rows miss, and every shape compares its result with the feature off and reports
# whether records were staged. `max_threads_min_free_memory_per_thread = 0` keeps the four threads whatever the free
# memory. The queries run in their own clickhouse-local process, so the counters in `system.events` belong to them
# alone.
function run_shape()
{
    local label=$1 query=$2
    echo "$label"
    $CLICKHOUSE_LOCAL --query "
    SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 8192, allow_suspicious_types_in_group_by = 1;
    SET adaptive_aggregator_freeze_threshold = 1000, adaptive_aggregator_freeze_threshold_bytes = 0, adaptive_aggregator_disable_thaw = 1;
    SET collect_hash_table_stats_during_aggregation = 0;
    SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
    SELECT 'matches the baseline',
        (${query//__SETTINGS__/enable_adaptive_aggregator = 0}) = (${query//__SETTINGS__/enable_adaptive_aggregator = 1});
    SELECT 'records staged',
        (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationStagedRecords') > 0;
    "
}

dynamic_key="if(number % 2 = 0, (number % 100003)::Dynamic, toString(number % 100003)::Dynamic) AS d"
wide_keys="concat(repeat('a-group-key-wider-than-batches-of-serialized-keys-', 3), toString(number % 100003)) AS a, number % 7 AS b"

run_shape 'Dynamic key without aggregates' "SELECT count(), sum(cityHash64(toString(d))) FROM (
    SELECT ${dynamic_key} FROM numbers_mt(200000) GROUP BY d SETTINGS __SETTINGS__)"

run_shape 'Dynamic key with a count' "SELECT count(), sum(cityHash64(toString(d), c)) FROM (
    SELECT ${dynamic_key}, count() AS c FROM numbers_mt(200000) GROUP BY d SETTINGS __SETTINGS__)"

run_shape 'Dynamic key with arguments' "SELECT count(), sum(cityHash64(toString(d), s, m)) FROM (
    SELECT ${dynamic_key}, sum(number) AS s, max(toString(number)) AS m FROM numbers_mt(200000) GROUP BY d
    SETTINGS __SETTINGS__)"

run_shape 'Wide string and number keys with arguments' "SELECT count(), sum(cityHash64(a, b, s, m)) FROM (
    SELECT ${wide_keys}, sum(number) AS s, max(toString(number)) AS m FROM numbers_mt(200000) GROUP BY a, b
    SETTINGS __SETTINGS__)"
