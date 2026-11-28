#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The adaptive aggregator takes an aggregation of one input stream, which `max_threads = 1` gives, and an
# aggregation of several streams with one thread. A producer freezes its table at the freeze threshold and
# stages the rows of new keys, and the adaptive merge drains them. Every cell runs its query with the feature
# off and on and compares the row count and a hash of all rows. The counters of a cell are the change of
# `system.events` over it, so all cells run in one clickhouse-local process of their own.
#
# The cells cover the key methods and the merges of a single producer: a fixed-width key with arguments, a
# `String` key, whose table converts to two-level when it froze, two `String` keys, which serialize, a
# key-only aggregation, a top-K by `count` that the merge prunes, an aggregation below the freeze threshold,
# and a spilling one. A single stream aggregates with two-level aggregation off, and the adaptive aggregator
# takes such an aggregation only when it is final. So the partial aggregation under `WITH TOTALS` stays on
# the ordinary path, because the merge of partial results with two-level aggregation off relies on
# single-level blocks. The last two cells read two `UNION ALL` streams with one thread, with two-level
# aggregation on and off.

settings="max_threads = 1, max_block_size = 8192, compile_aggregate_expressions = 0,
    optimize_aggregation_in_order = 0, collect_hash_table_stats_during_aggregation = 0,
    group_by_two_level_threshold = 100000, group_by_two_level_threshold_bytes = 50000000,
    max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0,
    adaptive_aggregator_freeze_threshold = 16384, adaptive_aggregator_freeze_threshold_bytes = 4194304"

# Prints the three queries of a cell: the query with the feature off, with it on, and the counters so far.
function cell()
{
    local name=$1 query=$2 extra=${3:+, $3}
    echo "SELECT '$name', count(), sum(cityHash64(*)) FROM ($query) SETTINGS enable_adaptive_aggregator = 0$extra;"
    echo "SELECT '$name', count(), sum(cityHash64(*)) FROM ($query) SETTINGS enable_adaptive_aggregator = 1$extra;"
    echo "SELECT '$name', sumIf(value, event = 'AdaptiveAggregationLocalFreezes'),
        sumIf(value, event = 'AdaptiveAggregationSpills'), sumIf(value, event = 'AdaptiveAggregationPrunedRecords')
        FROM system.events;"
}

function queries()
{
    echo "SET $settings;"
    cell 'fixed-width key' \
        "SELECT toUInt64(number % 100000) AS k, count(), sum(number), avg(number) FROM numbers(200000) GROUP BY k"
    cell 'String key' "SELECT toString(number % 100000) AS s, count() FROM numbers(200000) GROUP BY s"
    cell 'two String keys' \
        "SELECT toString(number % 1000) AS a, toString(number % 997) AS b, count(), sum(number)
        FROM numbers(200000) GROUP BY a, b"
    cell 'key only' "SELECT toUInt64(number % 100000) AS k FROM numbers(200000) GROUP BY k"
    # Keys 0 to 99 repeat 1 to 100 times, so the 10 best counts are distinct, and all other keys are unique.
    cell 'top-K by count' \
        "SELECT k, count() AS c FROM (SELECT
        if(number < 5050, toUInt64(floor((sqrt(8 * number + 1) - 1) / 2)), toUInt64(number)) AS k
        FROM numbers(200000)) GROUP BY k ORDER BY c DESC LIMIT 10"
    cell 'below the freeze threshold' "SELECT toUInt64(number % 1000) AS k, count() FROM numbers(200000) GROUP BY k"
    # The staged records of this cell take several MiB, so the producer writes them to disk many times.
    cell 'spilling' "SELECT toUInt64(number % 300000) AS k, count(), sum(number) FROM numbers(600000) GROUP BY k" \
        "max_bytes_before_external_group_by = '1Mi'"
    cell 'WITH TOTALS' \
        "SELECT toUInt64(number % 100000) AS k, count() FROM numbers(200000) GROUP BY k WITH TOTALS"
    local two_streams="SELECT k, count(), sum(v) FROM (SELECT toUInt64(number) AS k, number AS v FROM numbers(0, 100000)
        UNION ALL SELECT toUInt64(number), number FROM numbers(50000, 100000)) GROUP BY k"
    cell 'two streams' "$two_streams"
    cell 'two streams, two-level off' "$two_streams" \
        "group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0"
}

$CLICKHOUSE_LOCAL --query "$(queries)" | awk -F'\t' '
    NR % 3 == 1 { off = $2 " " $3; next }
    NR % 3 == 2 { on = $2 " " $3; next }
    {
        printf "%s\t%s\t%s\t%s\t%s\n", $1, (on == off ? "same result" : "different result: " off " and " on),
            ($2 > freezes ? "froze" : "did not freeze"), ($3 > spills ? "spilled" : "did not spill"),
            ($4 > pruned ? "pruned" : "did not prune");
        freezes = $2; spills = $3; pruned = $4;
    }'
