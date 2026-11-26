#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A frozen producer of the adaptive aggregator adds the repeats of a missed key to the count of the key's
# record after the record is written, while the count keeps the width that the record stores: one byte for a
# fixed-width key, a staged `UInt` of one byte below 255 for a string key, and the same byte holding the count
# above a flag for a key of fixed-width values and a string. A repeat that would widen the count stages a
# record of its own. Here each lane brings 300 keys, freezes its table once it holds 256 of them, and then
# repeats one missed key for 600 rows in blocks of 12 rows, so the key's written record fills its count and
# later rows start new records. Each key kind prints whether its counts equal those of the ordinary
# aggregation and whether rows combined. The empty string takes the path of a key that repeats the last missed
# key without a lookup.
#
# Everything runs in one clickhouse-local process, so the combined-row counter in `system.events` belongs to
# these queries, and each query's share is the difference of the counter around it. Four UNION ALL lanes feed
# the four producers, one each.

ROWS="SELECT if(number % 900 < 300, number, 1000000 + intDiv(number, 900)) AS k, number % 900 >= 300 AS missed FROM (
    SELECT number FROM numbers(0, 900) UNION ALL SELECT number FROM numbers(900, 900)
    UNION ALL SELECT number FROM numbers(1800, 900) UNION ALL SELECT number FROM numbers(2700, 900))"

queries=""
counter="SELECT sum(value) FROM system.events WHERE event = 'AdaptiveAggregationCombinedRows'"
function add_shape()
{
    local label=$1 keys=$2
    local query="SELECT ${keys}, count() FROM (${ROWS}) GROUP BY ${keys} SETTINGS enable_adaptive_aggregator = __ADAPTIVE__"
    queries+="
    SELECT 'result', '${label}', count(), sum(cityHash64(*)) FROM (${query//__ADAPTIVE__/0});
    SELECT 'result', '${label}', count(), sum(cityHash64(*)) FROM (${query//__ADAPTIVE__/1});
    SELECT 'combined', '${label}', (${counter});"
}

add_shape "fixed-width key" "k"
add_shape "string key" "toString(k) AS s"
add_shape "fixed-width values and a string" "k, toString(missed) AS s"
add_shape "fixed-width values and an empty string" "k, if(missed, '', 'learned') AS s"

$CLICKHOUSE_LOCAL --query "
SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 12;
SET adaptive_aggregator_freeze_threshold = 256, adaptive_aggregator_freeze_threshold_bytes = 0;
SET adaptive_aggregator_disable_thaw = 1, collect_hash_table_stats_during_aggregation = 0;
SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 1000000000;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
SET optimize_aggregation_in_order = 0;
-- The string keys stay keys of their own rather than functions of the other keys or of their arguments.
SET optimize_injective_functions_in_group_by = 0, optimize_group_by_function_keys = 0;
${queries}
" | awk -F'\t' '
    # A shape prints its result with the adaptive aggregator off, then on, and the cumulative combined-row counter.
    $1 == "result" { result[$2, ++results[$2]] = $3 "\t" $4; next }
    $1 == "combined" {
        print $2 "\tresults match: " (result[$2, 1] == result[$2, 2]) "\tcombined: " ($3 - previous > 0)
        previous = $3
        next
    }'
