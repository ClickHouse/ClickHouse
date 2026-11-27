#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A frozen producer of the adaptive aggregator looks its missed rows up in the tails of their partitions only while
# the lookups find their keys. A gate closes the tails when they stop, and reopens them on evidence, in two ways:
# - The watch. The first 4096 rows of a lane have keys of their own, so the tails close. In the rest of the lane, the
#   rows of two keys take turns, which only a tail combines, because no row repeats the previous one. The closed
#   gate compares each record that the writer appends with the previous record of its partition, sees the repeats
#   and reopens the tails, which then combine rows.
# - The try. Closed tails also reopen after 1048576 missed rows, whatever the watch found. One lane has 1060000 rows
#   of keys of their own, so its tails close, reopen for the try and close again.
# Each shape prints whether its results equal those of the ordinary aggregation, how many times the tails closed,
# and whether rows combined.
#
# Everything runs in one clickhouse-local process, so the counters in `system.events` belong to these queries, and
# each query's share is the difference of the counters around it. Four UNION ALL lanes feed the four producers, one
# each, and every local table freezes at its first key.

WATCH_ROWS="SELECT if(number % 12288 < 4096, number, 1000000000 + (number % 2) * 1000000 + intDiv(number, 16)) AS k FROM (
    SELECT number FROM numbers(0, 12288) UNION ALL SELECT number FROM numbers(12288, 12288)
    UNION ALL SELECT number FROM numbers(24576, 12288) UNION ALL SELECT number FROM numbers(36864, 12288))"
TRY_ROWS="SELECT number AS k FROM (
    SELECT number FROM numbers(0, 1060000) UNION ALL SELECT number FROM numbers(2000000, 100)
    UNION ALL SELECT number FROM numbers(3000000, 100) UNION ALL SELECT number FROM numbers(4000000, 100))"

queries=""
counters="SELECT sumIf(value, event = 'AdaptiveAggregationTailClosures'), sumIf(value, event = 'AdaptiveAggregationCombinedRows')
    FROM system.events"
function add_shape()
{
    local label=$1 query=$2
    queries+="
    SELECT 'result', '${label}', count(), sum(cityHash64(*)) FROM (${query//__ADAPTIVE__/0});
    SELECT 'result', '${label}', count(), sum(cityHash64(*)) FROM (${query//__ADAPTIVE__/1});
    SELECT 'counters', '${label}', * FROM (${counters});"
}

add_shape "watch" "SELECT k, count() FROM (${WATCH_ROWS}) GROUP BY k SETTINGS enable_adaptive_aggregator = __ADAPTIVE__"
add_shape "try" "SELECT k FROM (${TRY_ROWS}) GROUP BY k SETTINGS enable_adaptive_aggregator = __ADAPTIVE__"

$CLICKHOUSE_LOCAL --query "
SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 1000;
SET adaptive_aggregator_freeze_threshold = 1, adaptive_aggregator_freeze_threshold_bytes = 0;
SET adaptive_aggregator_disable_thaw = 1, collect_hash_table_stats_during_aggregation = 0;
SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 1000000000;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
SET optimize_aggregation_in_order = 0;
${queries}
" | awk -F'\t' '
    # A shape prints its result with the adaptive aggregator off, then on, and the cumulative counters.
    $1 == "result" { result[$2, ++results[$2]] = $3 "\t" $4; next }
    $1 == "counters" {
        print $2 "\tresults match: " (result[$2, 1] == result[$2, 2]) "\tclosures: " ($3 - closures) "\tcombined: " ($4 - combined > 0)
        closures = $3
        combined = $4
    }'
