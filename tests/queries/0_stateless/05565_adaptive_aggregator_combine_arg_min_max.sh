#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With `adaptive_aggregator_combine_arg_min_max`, a frozen producer of the adaptive aggregator combines the
# missed rows of a key into one record, of their best row, when every aggregate function selects one row by
# one ordering column. Each shape runs with the setting off and then on, and prints whether the results match
# and whether the setting staged fewer records. The shapes cover every ordering type with the largest value
# preferred, with values on both sides of the sign bit, the smallest value preferred for the types whose ranks
# differ in kind, `any` and `count` next to the selecting aggregates, and each kind of aggregate as the source
# of the ordering column.
#
# Everything runs in one clickhouse-local process, so the staged-record counter in `system.events` belongs to
# these queries, and each query's share is the difference of the counter around it. Four UNION ALL lanes feed
# the four producers, one each, so the blocks and the frozen tables do not depend on the scheduling. A key
# holds 4 consecutive rows of a lane, ordered without ties by `o`, so every group has one best row and the
# results are exact.

ROWS="SELECT number AS x, intDiv(number, 4) AS k, (number * 3 + intDiv(number, 4)) % 4 AS o FROM (
    SELECT number FROM numbers(0, 20000) UNION ALL SELECT number FROM numbers(20000, 20000)
    UNION ALL SELECT number FROM numbers(40000, 20000) UNION ALL SELECT number FROM numbers(60000, 20000))"

queries=""
function add_shape()
{
    local label=$1 aggregates=$2 rows=${3:-$ROWS}
    for value in 0 1; do
        queries+="
        SELECT 'result', '${label}', count(), sum(cityHash64(*)) FROM (
            SELECT k, ${aggregates} FROM (${rows}) GROUP BY k SETTINGS adaptive_aggregator_combine_arg_min_max = ${value});
        SELECT 'staged', '${label}', sum(value) FROM system.events WHERE event = 'AdaptiveAggregationStagedRecords';"
    done
}

# The ordering types, each with values that cross the sign bit of its width or of `Int64`. Each expression
# ends in a conversion, not in arithmetic: `max(e - c)` is rewritten to `max(e) - c`, so `max` and `argMax`
# would read different columns and the query would not qualify.
declare -A orderings=(
    [UInt8]="toUInt8(o * 60 + 10)"
    [UInt16]="toUInt16(o * 20000)"
    [UInt32]="toUInt32(o * 1400000000)"
    [UInt64]="if(o < 2, toUInt64(o), toUInt64(9223372036854775808) + o)"
    [Int8]="toInt8(toInt16(o) - 2)"
    [Int16]="toInt16(toInt32(o) * 1000 - 2000)"
    [Int32]="toInt32(toInt64(o) * 1000000 - 2000000)"
    [Int64]="toInt64(multiIf(o = 0, -8000000000000000000, o = 1, -4000000000000000000, o = 2, 0, 4000000000000000000))"
    [Date]="toDate(18000 + o)"
    [Date32]="toDate32(-25000 + o * 10000)"
    [DateTime]="toDateTime(1700000000 + o * 1000, 'UTC')"
    [DateTime64]="toDateTime64(o * 50 - 100, 3, 'UTC')"
    [Enum8]="CAST(toInt8(toInt16(o) - 2), 'Enum8(''a'' = -2, ''b'' = -1, ''c'' = 0, ''d'' = 1)')"
    [Enum16]="CAST(toInt16(toInt32(o) * 1000 - 1000), 'Enum16(''a'' = -1000, ''b'' = 0, ''c'' = 1000, ''d'' = 2000)')"
    [IPv4]="toIPv4(toUInt32(o * 1400000000))"
    [Time]="toTime(toInt32(o) * 1000 - 2000)"
    [Time64]="toTime64(toInt64(o) - 2, 3)"
    [Interval]="toIntervalSecond(toInt64(o) - 2)"
    [Decimal32]="toDecimal32(toInt64(o) - 2, 2)"
    [Decimal64]="toDecimal64(toInt64(o) * 1000000000 - 2000000000, 3)"
)
for type in UInt8 UInt16 UInt32 UInt64 Int8 Int16 Int32 Int64 Date Date32 DateTime DateTime64 Enum8 Enum16 IPv4 Time \
    Time64 Interval Decimal32 Decimal64; do
    t=${orderings[$type]}
    add_shape "$type, largest" "argMax(x, $t), max($t), count()"
done
# The smallest value is preferred through the complement of the rank, which every type takes alike, so the
# shapes cover the three kinds of rank: an unsigned 64-bit value with its top bit flipped, a signed value, and
# the ticks of a `DateTime64`.
for type in UInt64 Int64 DateTime64; do
    t=${orderings[$type]}
    add_shape "$type, smallest" "argMin(x, $t), min($t), count()"
done

# `any` of a value that is the same in all rows of a group, several `count`s, no `count`, and `max` or `min`
# as the first selecting aggregate, which makes its single argument the source of the ordering column.
add_shape "any next to argMax" "argMax(x, o), any(k * 2)"
add_shape "two counts next to argMin" "argMin(x, o), count(), count(x)"
add_shape "argMax alone" "argMax(x, o)"
add_shape "max before argMax" "max(o), argMax(x, o)"
add_shape "min alone" "min(o)"

# A producer stops probing its frozen table once a sample of 65536 rows has almost no hits, and stages the
# remaining misses through the loop without the probe. The lanes of this shape are long enough for that.
LONG_ROWS="SELECT number AS x, intDiv(number, 4) AS k, (number * 3 + intDiv(number, 4)) % 4 AS o FROM (
    SELECT number FROM numbers(0, 100000) UNION ALL SELECT number FROM numbers(100000, 100000)
    UNION ALL SELECT number FROM numbers(200000, 100000) UNION ALL SELECT number FROM numbers(300000, 100000))"
add_shape "probing bypassed" "argMax(x, o), max(o), count()" "$LONG_ROWS"
queries+="
SELECT 'check', 'probing bypassed', (SELECT sum(value) FROM system.events WHERE event = 'AdaptiveAggregationProbeBypasses') > 0;"

$CLICKHOUSE_LOCAL --query "
SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 8192;
SET enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 1024, adaptive_aggregator_freeze_threshold_bytes = 0;
SET enable_time_time64_type = 1;
SET adaptive_aggregator_disable_thaw = 1, collect_hash_table_stats_during_aggregation = 0;
SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 1000000000;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
SET optimize_aggregation_in_order = 0;
${queries}
" | awk -F'\t' '
    # Each shape prints the result and the cumulative staged-record counter, first with the setting off, then
    # on.
    $1 == "result" { result[++results] = $3 "\t" $4; next }
    $1 == "staged" { staged[++counters] = $3 - previous; previous = $3; label[counters] = $2; next }
    $1 == "check" { checks = checks $2 "\t" $3 "\n" }
    END {
        for (i = 1; i < counters; i += 2)
            print label[i] "\tresults match: " (result[i] == result[i + 1]) "\tfewer records: " (staged[i + 1] < staged[i])
        printf "%s", checks
    }'
