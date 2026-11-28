#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `GROUP BY` without aggregate functions stages the misses of a frozen table as key-only records, and the repeats of a
# missed key combine into its record, as the rows of a lone `count` do. The keys come in runs of 300 equal rows, which
# stay together within the blocks of `numbers_mt`, and the frozen tables hold only the first keys, so nearly every run
# is staged as one record: a few thousand records where about 570,000 rows miss, and each check asks for fewer than a
# tenth of the rows. A `String` key takes the map method without aggregates, a `UInt64` key and a `UInt64, String` key
# take set methods. The methods of fixed-width keys and one `String` key have no set form, so they are off, and the
# `UInt64, String` key takes the serialized set method. The interleaved `String` key repeats each of four keys every
# fourth row, so no run forms, and the per-partition cache of the last missed key combines the repeats instead. Each
# check compares the result with the one of the feature off; it runs in a clickhouse-local process of its own, so the
# counters in `system.events` belong to it alone.
function check()
{
    local label=$1 query=$2
    $CLICKHOUSE_LOCAL --query "
    SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 8192;
    SET enable_adaptive_aggregator = 1, adaptive_aggregator_disable_thaw = 1, collect_hash_table_stats_during_aggregation = 0;
    SET adaptive_aggregator_freeze_threshold = 16, adaptive_aggregator_freeze_threshold_bytes = 0;
    SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 500000000;
    SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0, optimize_aggregation_in_order = 0;
    SET enable_fixed_and_packed_string_keys_in_aggregation = 0;

    SELECT '$label',
        (SELECT count(), sum(cityHash64(*)) FROM ($query)) = (SELECT count(), sum(cityHash64(*)) FROM ($query SETTINGS enable_adaptive_aggregator = 0)),
        (SELECT sumIf(value, event = 'AdaptiveAggregationStagedRecords') BETWEEN 1 AND 60000 FROM system.events);
    " | paste -sd '\t'
}

check 'String key' "SELECT k FROM (SELECT toString(intDiv(number, 300)) AS k FROM numbers_mt(600000)) GROUP BY k"
check 'UInt64 key' "SELECT k FROM (SELECT intDiv(number, 300) AS k FROM numbers_mt(600000)) GROUP BY k"
check 'UInt64, String key' "SELECT k, s FROM (SELECT intDiv(number, 300) AS k, toString(k) AS s FROM numbers_mt(600000)) GROUP BY k, s"
check 'Interleaved String key' "SELECT k FROM (SELECT toString(intDiv(number, 1200) * 4 + number % 4) AS k FROM numbers_mt(600000)) GROUP BY k"
