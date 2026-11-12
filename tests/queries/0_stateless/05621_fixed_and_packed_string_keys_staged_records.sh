#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The staged records of the adaptive aggregator with the methods of fixed-width keys and one `String` key. The
# head of a record is a staged `UInt`: its low bit tells whether the string is non-empty, and the bits above
# hold the count of a count record. A head from 255 on takes the escape byte 255 and four more bytes. A run of
# 127 rows needs the long form with a non-empty string, because its head equals the escape byte, and keeps the
# short form with an empty string. The keys come in runs of equal rows, which stay together within the blocks
# of `numbers_mt`, and the frozen tables hold only the first keys, so nearly every run is staged as one
# record. A repeat of a missed key with an empty string joins the record of its run without being hashed. The
# interleaved keys repeat each of four keys every fourth row, so no run forms, and the per-partition cache of
# the last missed key combines the repeats instead. Each check compares the result with the one of the
# ordinary aggregator with the serialized method. It requires staged records, but fewer than a tenth of the
# rows, so it cannot pass without the staging or without the combining. It also requires the merge to drain
# exactly the staged records, because the count of a record never splits. Each check runs in a
# clickhouse-local process of its own, so the counters in `system.events` belong to it alone.
function check()
{
    local label=$1 query=$2
    $CLICKHOUSE_LOCAL --query "
    SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 8192;
    SET enable_adaptive_aggregator = 1, adaptive_aggregator_disable_thaw = 1, collect_hash_table_stats_during_aggregation = 0;
    SET adaptive_aggregator_freeze_threshold = 16, adaptive_aggregator_freeze_threshold_bytes = 0;
    SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 500000000;
    SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0, optimize_aggregation_in_order = 0;
    SET enable_fixed_and_packed_string_keys_in_aggregation = 1;

    SELECT '$label',
        (SELECT count(), sum(cityHash64(*)) FROM ($query))
            = (SELECT count(), sum(cityHash64(*)) FROM ($query
                SETTINGS enable_adaptive_aggregator = 0, enable_fixed_and_packed_string_keys_in_aggregation = 0)),
        (SELECT staged BETWEEN 1 AND 60000 AND drained = staged
            FROM (SELECT sumIf(value, event = 'AdaptiveAggregationStagedRecords') AS staged,
                sumIf(value, event = 'AdaptiveAggregationDrainedRecords') AS drained FROM system.events));
    " | paste -sd '\t'
}

check 'Count, runs of 127' "SELECT k, s, count() AS c FROM (SELECT intDiv(number, 127) AS k, if(k % 2 = 0, '', toString(k)) AS s FROM numbers_mt(600000)) GROUP BY k, s"
check 'Key-only, runs of 300' "SELECT k, s FROM (SELECT intDiv(number, 300) AS k, if(k % 2 = 0, '', toString(k)) AS s FROM numbers_mt(600000)) GROUP BY k, s"
check 'Count, interleaved' "SELECT k, s, count() AS c FROM (SELECT intDiv(number, 1200) AS k, toString(number % 4) AS s FROM numbers_mt(600000)) GROUP BY k, s"
