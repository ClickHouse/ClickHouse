#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Which aggregations `adaptive_aggregator_combine_arg_min_max` applies to. The aggregations whose aggregate
# functions do not all select one row by one ordering column of a type stored as an integer of up to 64 bits
# keep one staged record per missed row, and so does the bucket top-K, which counts the staged records of a
# key as its rows. The other top-K modes and `HAVING` combine the misses. Each shape runs with the setting off
# and then on, and prints whether the results match and, unless it stages no records, whether the setting
# staged fewer records or the same records.
#
# Everything runs in one clickhouse-local process, so the staged-record counter in `system.events` belongs to
# these queries, and each query's share is the difference of the counter around it. Four UNION ALL lanes feed
# the four producers, one each, so the blocks and the frozen tables do not depend on the scheduling and both
# runs of a shape that does not combine stage the same records. A key holds 4 consecutive rows of a lane,
# ordered without ties by `o`.

ROWS="SELECT number AS x, intDiv(number, 4) AS k, (number * 3 + intDiv(number, 4)) % 4 AS o FROM (
    SELECT number FROM numbers(0, 20000) UNION ALL SELECT number FROM numbers(20000, 20000)
    UNION ALL SELECT number FROM numbers(40000, 20000) UNION ALL SELECT number FROM numbers(60000, 20000))"

queries=""
function add_shape()
{
    local label=$1 expected=$2 outer=$3 inner=$4
    for value in 0 1; do
        queries+="
        SELECT 'result', '${label}', '${expected}', ${outer} FROM (${inner//__VALUE__/$value});
        SELECT 'staged', '${label}', '${expected}', sum(value) FROM system.events WHERE event = 'AdaptiveAggregationStagedRecords';"
    done
}

function grouped()
{
    local aggregates=$1 tail=$2
    echo "SELECT k, ${aggregates} FROM (${ROWS}) GROUP BY k ${tail} SETTINGS adaptive_aggregator_combine_arg_min_max = __VALUE__"
}

HASH="count(), sum(cityHash64(*))"
add_shape "argMax and argMin" same "$HASH" "$(grouped "argMax(x, o), argMin(x, o)")"
add_shape "two ordering columns" same "$HASH" "$(grouped "argMax(x, o), max(x)")"
add_shape "Nullable argument" same "$HASH" "$(grouped "argMax(toNullable(x), o), max(o)")"
add_shape "combinator" same "$HASH" "$(grouped "argMaxIf(x, o, x % 3 = 0), max(o)")"
add_shape "another aggregate function" same "$HASH" "$(grouped "argMax(x, o), sum(x)")"
add_shape "Float64 ordering" same "$HASH" "$(grouped "argMax(x, toFloat64(o)), max(toFloat64(o))")"
add_shape "String ordering" same "$HASH" "$(grouped "argMax(x, toString(o)), max(toString(o))")"
add_shape "any and count only" same "$HASH" "$(grouped "any(k * 2), count()")"
# Every group counts 4 rows, so which 10 the top-K returns is not defined, while their number and counts are.
add_shape "bucket top-K by count" same "count(), sum(c), min(a >= 0)" "$(grouped "argMax(x, o) AS a, count() AS c" "ORDER BY c DESC LIMIT 10")"
add_shape "top-K by the ordering value" fewer "$HASH" "$(grouped "argMax(o, x), max(x) AS m" "ORDER BY m DESC LIMIT 10")"
# The heap of the top-K by the key keeps the tables below the freeze threshold here, so only the results are
# compared.
add_shape "top-K by the key" results "$HASH" "$(grouped "argMax(x, o), max(o)" "ORDER BY k DESC LIMIT 10")"
add_shape "HAVING" fewer "$HASH" "$(grouped "argMax(x, o) AS a, count()" "HAVING a % 2 = 0")"

$CLICKHOUSE_LOCAL --query "
SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 8192;
SET enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 1024, adaptive_aggregator_freeze_threshold_bytes = 0;
SET adaptive_aggregator_disable_thaw = 1, collect_hash_table_stats_during_aggregation = 0;
SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 1000000000;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
SET optimize_aggregation_in_order = 0, max_rows_to_group_by = 0;
${queries}
" | awk -F'\t' '
    # A shape prints its result and the cumulative staged-record counter with the setting off, then on.
    $1 == "result" { result[$2, ++results[$2]] = substr($0, length($1 $2 $3) + 4); next }
    $1 == "staged" {
        staged[$2, ++counters[$2]] = $4 - previous
        previous = $4
        if (counters[$2] < 2)
            next
        line = $2 "\tresults match: " (result[$2, 1] == result[$2, 2])
        if ($3 == "fewer")
            line = line "\tfewer records: " (staged[$2, 2] < staged[$2, 1])
        else if ($3 == "same")
            line = line "\tsame records: " (staged[$2, 2] == staged[$2, 1] && staged[$2, 1] > 0)
        print line
    }'
