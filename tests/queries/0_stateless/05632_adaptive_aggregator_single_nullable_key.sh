#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The adaptive aggregator takes a single `Nullable` key, whose method keeps the NULL key in a slot of its own outside
# the cells of the table. A frozen table takes every row of the NULL key into the slot without a lookup, and the first
# merge unit of bucket 0 takes the slots of all producers. Every cell runs its query with the feature off and on and
# compares the row count and a hash of all rows. The counters of a cell are the change of `system.events` over it, so
# all cells run in one clickhouse-local process of their own.
#
# A key is NULL in one row of seven. Each lane of `UNION ALL` feeds one producer. In the mixed keys, half of the other
# rows repeat 100 hot keys and half are new, so the frozen table keeps its probe. In the unique keys every other row is
# new, so the probe is bypassed. The cells cover each kernel of a frozen table: a lone `count`, aggregate functions with
# arguments, and no aggregate functions, with the probe and without it. A bypassed probe needs places for the NULL
# rows, so a block without NULL keys takes the shape without places. They cover the keys of `UInt32`, `UInt64`,
# `String` and `FixedString`, large states of the NULL key, constant NULL keys, producers that hold only the NULL key,
# tables that never freeze, the external aggregation, and the partial aggregation under `WITH TOTALS`.

settings="max_threads = 2, max_threads_min_free_memory_per_thread = 0, max_block_size = 8192,
    compile_aggregate_expressions = 0, optimize_aggregation_in_order = 0, collect_hash_table_stats_during_aggregation = 0,
    group_by_two_level_threshold = 100000, group_by_two_level_threshold_bytes = 50000000,
    max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0,
    adaptive_aggregator_freeze_threshold = 1024, adaptive_aggregator_freeze_threshold_bytes = 4194304"

# Prints the three queries of a cell: the query with the feature off, with it on, and the counters so far.
function cell()
{
    local name=$1 query=$2 extra=${3:+, $3}
    echo "SELECT '$name', count(), sum(cityHash64(*)) FROM ($query) SETTINGS enable_adaptive_aggregator = 0$extra;"
    echo "SELECT '$name', count(), sum(cityHash64(*)) FROM ($query) SETTINGS enable_adaptive_aggregator = 1$extra;"
    echo "SELECT '$name', sumIf(value, event = 'AdaptiveAggregationLocalFreezes'),
        sumIf(value, event = 'AdaptiveAggregationSpills'), sumIf(value, event = 'AdaptiveAggregationProbeBypasses')
        FROM system.events;"
}

# Two lanes of `rows` rows each, with the key `k` from `key` and the value `v`.
function lanes()
{
    local key=$1 rows=${2:-150000}
    echo "SELECT ${key//NUMBER/number} AS k, number AS v FROM numbers(0, $rows)
        UNION ALL SELECT ${key//NUMBER/number}, number FROM numbers($rows, $rows)"
}

mixed()
{
    echo "if(NUMBER % 7 = 0, NULL, if(NUMBER % 2 = 0, $1(NUMBER % 100), $1(NUMBER)))"
}
unique="if(NUMBER % 7 = 0, NULL, NUMBER)"

function queries()
{
    echo "SET $settings;"
    cell 'count' "SELECT k, count() FROM ($(lanes "$(mixed toUInt64)")) GROUP BY k"
    cell 'count, probe bypassed' "SELECT k, count() FROM ($(lanes "$unique")) GROUP BY k"
    cell 'arguments' "SELECT k, sum(v), avg(v) FROM ($(lanes "$(mixed toUInt64)")) GROUP BY k"
    cell 'arguments, probe bypassed' "SELECT k, sum(v) FROM ($(lanes "$unique")) GROUP BY k"
    cell 'arguments, probe bypassed, no NULL key' "SELECT k, sum(v) FROM ($(lanes "toNullable(NUMBER)")) GROUP BY k"
    cell 'large states' "SELECT k, uniqExact(v) FROM ($(lanes "$(mixed toUInt64)")) GROUP BY k"
    cell 'key only' "SELECT k FROM ($(lanes "$(mixed toUInt64)")) GROUP BY k"
    cell 'key only, probe bypassed' "SELECT k FROM ($(lanes "$unique")) GROUP BY k"
    cell 'UInt32 key' "SELECT k, count(), sum(v) FROM ($(lanes "$(mixed toUInt32)")) GROUP BY k"
    cell 'String key' "SELECT k, count() FROM ($(lanes "$(mixed toString)")) GROUP BY k"
    cell 'String key, arguments' "SELECT k, max(toString(v)) FROM ($(lanes "$(mixed toString)")) GROUP BY k"
    cell 'String key only' "SELECT k FROM ($(lanes "$(mixed toString)")) GROUP BY k"
    cell 'FixedString key' \
        "SELECT k, count(), sum(v) FROM ($(lanes "toFixedString(leftPad(toString($(mixed toUInt64)), 8, '0'), 8)")) GROUP BY k"
    # The blocks of both branches spread over the producers, so a frozen producer takes blocks whose key column is a
    # constant NULL.
    cell 'constant NULL keys' "SELECT k, count(), sum(v) FROM (SELECT toNullable(number % 300000) AS k, number AS v
        FROM numbers_mt(600000) UNION ALL SELECT CAST(NULL AS Nullable(UInt64)), number FROM numbers_mt(600000)) GROUP BY k"
    cell 'a producer of the NULL key alone' "SELECT k, count(), sum(v) FROM (SELECT CAST(NULL AS Nullable(UInt64)) AS k,
        number AS v FROM numbers(0, 150000) UNION ALL SELECT toNullable(number), number FROM numbers(150000, 150000)) GROUP BY k"
    cell 'below the freeze threshold' "SELECT k, count(), sum(v) FROM ($(lanes "if(NUMBER % 7 = 0, NULL, NUMBER % 500)")) GROUP BY k"
    # The staged records of this cell take several MiB, so the producers write them to disk many times.
    cell 'spilling' "SELECT k, count(), sum(v) FROM ($(lanes "$unique" 300000)) GROUP BY k" "max_bytes_before_external_group_by = '1Mi'"
    cell 'WITH TOTALS' "SELECT k, count(), sum(v) FROM ($(lanes "$(mixed toUInt64)")) GROUP BY k WITH TOTALS"
}

$CLICKHOUSE_LOCAL --query "$(queries)" | awk -F'\t' '
    NR % 3 == 1 { off = $2 " " $3; next }
    NR % 3 == 2 { on = $2 " " $3; next }
    {
        printf "%s\t%s\t%s\t%s\t%s\n", $1, (on == off ? "same result" : "different result: " off " and " on),
            ($2 > freezes ? "froze" : "did not freeze"), ($3 > spills ? "spilled" : "did not spill"),
            ($4 > bypasses ? "bypassed" : "did not bypass");
        freezes = $2; spills = $3; bypasses = $4;
    }'
