#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The staged records of the adaptive aggregator with the methods of fixed-width keys and one `String` key. The
# lead of a count record is a staged `UInt`: its low bit tells whether the string is non-empty, and the bits
# above hold the count. A lead from 255 on takes the escape byte 255 and four more bytes. A run of 127 rows
# needs the long form with a non-empty string, because its lead equals the escape byte, and keeps the short
# form with an empty string. The lead of the other records is the size of the string. A record stores the
# bytes that the fixed-width key columns fill: 9 bytes of a 16-byte key, 1 or 2 bytes of a 4-byte key, 5 bytes
# of an 8-byte key, or all of them. The keys come in runs of equal rows, which stay together within the blocks
# of `numbers_mt`, and the frozen tables hold only the first keys, so nearly every run is staged as one
# record. A repeat of a missed key with an empty string joins the record of its run without being hashed. The
# interleaved keys repeat each of four keys every fourth row, so no run forms, and the per-partition cache of
# the last missed key combines the repeats instead. These checks require staged records, but fewer than a
# tenth of the rows, so they cannot pass without the staging or without the combining. They also require the
# merge to drain exactly the staged records, because the count of a record never splits. The argument records
# carry fixed-size arguments and a `String` argument, which the reader skips to find the end of a record, or
# an `Array` argument, which makes the record store its size. They have to be staged and drained. The last
# check writes the staged records of long strings to disk, which the merge copies when it reads them back.
# Each check compares the result with the one of the ordinary aggregator with the serialized method. It runs
# in a clickhouse-local process of its own, so the counters in `system.events` belong to it alone.
function check()
{
    local label=$1 query=$2 records=${3:-"staged BETWEEN 1 AND 60000 AND drained = staged"} settings=${4:-}
    $CLICKHOUSE_LOCAL --query "
    SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 8192;
    SET enable_adaptive_aggregator = 1, adaptive_aggregator_disable_thaw = 1, collect_hash_table_stats_during_aggregation = 0;
    SET adaptive_aggregator_freeze_threshold = 16, adaptive_aggregator_freeze_threshold_bytes = 0;
    SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 500000000;
    SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0, optimize_aggregation_in_order = 0;
    SET enable_fixed_and_packed_string_keys_in_aggregation = 1;
    $settings

    SELECT '$label',
        (SELECT count(), sum(cityHash64(*)) FROM ($query))
            = (SELECT count(), sum(cityHash64(*)) FROM ($query
                SETTINGS enable_adaptive_aggregator = 0, enable_fixed_and_packed_string_keys_in_aggregation = 0)),
        (SELECT $records
            FROM (SELECT sumIf(value, event = 'AdaptiveAggregationStagedRecords') AS staged,
                sumIf(value, event = 'AdaptiveAggregationDrainedRecords') AS drained,
                sumIf(value, event = 'AdaptiveAggregationSpills') AS spills FROM system.events));
    " | paste -sd '\t'
}

check 'Count, 9 of 16 bytes, runs of 127' "SELECT k, b, s, count() AS c FROM (SELECT intDiv(number, 127) AS k, toUInt8(k % 3) AS b, if(k % 2 = 0, '', toString(k)) AS s FROM numbers_mt(600000)) GROUP BY k, b, s"
check 'Key-only, 1 of 4 bytes, runs of 300' "SELECT b, s FROM (SELECT intDiv(number, 300) AS k, toUInt8(k % 251) AS b, if(k % 2 = 0, '', toString(k)) AS s FROM numbers_mt(600000)) GROUP BY b, s"
check 'Count, 8 of 8 bytes, interleaved' "SELECT k, s, count() AS c FROM (SELECT intDiv(number, 1200) AS k, toString(number % 4) AS s FROM numbers_mt(600000)) GROUP BY k, s"
check 'Fixed-size and String arguments, 5 of 8 bytes' "SELECT k, b, s, sum(x), max(t) FROM (SELECT toUInt32(intDiv(number, 3)) AS k, toUInt8(number % 3) AS b, if(number % 5 = 0, '', toString(k % 1000)) AS s, number AS x, toString(number % 977) AS t FROM numbers_mt(300000)) GROUP BY k, b, s" "staged > 0 AND drained = staged"
check 'An Array argument, 2 of 4 bytes' "SELECT b, s, max(a) FROM (SELECT toUInt16(intDiv(number, 3) % 60000) AS b, if(number % 5 = 0, '', toString(intDiv(number, 3) % 1000)) AS s, [number % 7, number % 11] AS a FROM numbers_mt(300000)) GROUP BY b, s" "staged > 0 AND drained = staged"
check 'Count, long strings, written to disk' "SELECT k, s, count() AS c FROM (SELECT intDiv(number, 3) AS k, concat(repeat('x', 20), toString(k % 1000)) AS s FROM numbers_mt(300000)) GROUP BY k, s" "staged > 0 AND spills > 0" "SET max_bytes_before_external_group_by = 1;"
