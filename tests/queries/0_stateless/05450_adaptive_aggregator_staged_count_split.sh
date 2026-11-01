#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A count record of the adaptive aggregator holds up to 255 rows of its key within a block, and the staging splits a
# larger count into several records, which the merge adds up. The keys come in runs of 300 equal rows, which stay
# together within the blocks of `numbers_mt`, and the frozen tables hold only the first keys, so nearly every run is
# staged as one count of 300 and split. The merge then drains more records than the staging counted misses. A `String`
# key stages its bytes and a `UInt64` key its fixed width. Each check compares the result with the one of the feature
# off; it runs in a clickhouse-local process of its own, so the counters in `system.events` belong to it alone.
function check()
{
    local label=$1 query=$2
    $CLICKHOUSE_LOCAL --query "
    SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 8192;
    SET enable_adaptive_aggregator = 1, adaptive_aggregator_disable_thaw = 1, collect_hash_table_stats_during_aggregation = 0;
    SET adaptive_aggregator_freeze_threshold = 16, adaptive_aggregator_freeze_threshold_bytes = 0;
    SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 500000000;
    SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0, optimize_aggregation_in_order = 0;

    SELECT '$label',
        (SELECT sum(cityHash64(*)) FROM ($query)) = (SELECT sum(cityHash64(*)) FROM ($query SETTINGS enable_adaptive_aggregator = 0)),
        (SELECT sumIf(value, event = 'AdaptiveAggregationDrainedRecords') > sumIf(value, event = 'AdaptiveAggregationStagedRecords')
            FROM system.events);
    " | paste -sd '\t'
}

check 'String key' "SELECT k, count() AS c FROM (SELECT toString(intDiv(number, 300)) AS k FROM numbers_mt(600000)) GROUP BY k"
check 'UInt64 key' "SELECT k, count() AS c FROM (SELECT intDiv(number, 300) AS k FROM numbers_mt(600000)) GROUP BY k"
