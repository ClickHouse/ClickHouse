#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The top-K of `GROUP BY ... ORDER BY <aggregate> LIMIT n` serves `Nullable` keys. A method of several keys stores
# the NULL flags in its keys. The method of a single `Nullable` key keeps the NULL key in a slot of its own outside
# the cells, so the NULL group has no hash and no count bin. The bucket conversion and the threshold merge rank it
# as a candidate. The adaptive merge gives it a bound of its own and offers its rank to the shared threshold before
# the merge.

head="number % 4 = 0, toUInt64(sqrt(number % 10000))"
inside="SELECT multiIf($head, number % 8 = 1, NULL, number + 1000000) AS k, number FROM numbers_mt(100000)"
outside="SELECT multiIf($head, number % 50000 = 1, NULL, number + 1000000) AS k, number FROM numbers_mt(100000)"
ties="SELECT multiIf($head, number = 1, NULL, number + 1000000) AS k, number FROM numbers_mt(100000)"
single_rows="SELECT multiIf(number < 9, number + 1000000, number = 9, NULL, toUInt64((number - 10) % 45000)) AS k, number
    FROM numbers_mt(90010)"
one_stream_inside="SELECT multiIf($head, number % 8 = 1, NULL, number + 1000000) AS k, number FROM numbers(100000)"
one_stream_outside="SELECT multiIf($head, number % 50000 = 1, NULL, number + 1000000) AS k, number FROM numbers(100000)"
function four_lanes()
{
    local lanes=()
    for offset in 0 25000 50000 75000; do
        lanes+=("SELECT multiIf($head, $1, NULL, number + 1000000) AS k, number FROM numbers($offset, 25000)")
    done
    local IFS=$'\n'
    echo "${lanes[*]}" | sed '2,$s/^/UNION ALL /'
}

settings="SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 8192,
    optimize_aggregation_in_order = 0, collect_hash_table_stats_during_aggregation = 0,
    adaptive_aggregator_freeze_threshold = 128, optimize_injective_functions_in_group_by = 0,
    query_plan_enable_optimizations = 1, query_plan_aggregation_bucket_top_k = 1, transform_null_in = 1;"
no_spill="max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0"
ordinary="enable_adaptive_aggregator = 0, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1,
    compile_aggregate_expressions = 0, $no_spill"
adaptive="enable_adaptive_aggregator = 1, group_by_two_level_threshold = 100000, group_by_two_level_threshold_bytes = 50000000,
    compile_aggregate_expressions = 0, $no_spill"
compiled="enable_adaptive_aggregator = 0, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1,
    compile_aggregate_expressions = 1, min_count_to_compile_aggregate_expression = 0, $no_spill"
# One stream makes its table two-level only when the external aggregation can run, so the spill threshold is too high
# to trip.
one_stream="enable_adaptive_aggregator = 0, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1,
    compile_aggregate_expressions = 0, max_bytes_before_external_group_by = 10737418240,
    max_bytes_ratio_before_external_group_by = 0"

# The counters that say whether a cell's mechanism ran, by their position in the counters line.
conversion=1 threshold_merge=2 pruning=3 count_first=4

# Prints the queries of a cell `label|counter|path|source|keys|aggregates|columns|order`: the check of the top, then
# the counters so far.
function cell()
{
    local label counter path source keys aggregates columns order
    IFS='|' read -r label counter path source keys aggregates columns order <<< "$1"
    local query="SELECT $keys, $aggregates FROM (${!source}) GROUP BY $keys ORDER BY $order"
    echo "SET ${!path};
    SELECT '$label', ${!counter},
        (SELECT arraySort(groupArray(c)) FROM ($query))
            = (SELECT arraySort(groupArray(c)) FROM ($query SETTINGS query_plan_aggregation_bucket_top_k = 0)),
        (SELECT count() FROM ($query) WHERE ($columns) NOT IN (SELECT $keys, $aggregates FROM (${!source}) GROUP BY $keys)) = 0;
    SELECT sumIf(value, event = 'AggregationBucketTopKConversions'), sumIf(value, event = 'AggregationThresholdTopKMerges'),
        sumIf(value, event = 'AdaptiveAggregationPrunedUnits'), sumIf(value, event = 'AdaptiveAggregationCountFirstUnits')
        FROM system.events;"
}

four_lanes_inside=$(four_lanes "number % 8 = 1")
four_lanes_outside=$(four_lanes "number % 50000 = 1")

cells=(
    'Conversion, count, NULL inside|conversion|ordinary|inside|k|count() AS c|k, c|c DESC LIMIT 10'
    'Conversion, count, NULL outside|conversion|ordinary|outside|k|count() AS c|k, c|c DESC LIMIT 10'
    'Conversion, uniqExact, NULL inside|conversion|ordinary|inside|k|uniqExact(number) AS c|k, c|c DESC LIMIT 10'
    'Conversion, uniqExact, NULL outside|conversion|ordinary|outside|k|uniqExact(number) AS c|k, c|c DESC LIMIT 10'
    'Conversion, count with states, NULL inside|conversion|ordinary|four_lanes_inside|k|count() AS c, uniqExact(number % 7) AS u|k, c, u|c DESC LIMIT 10'
    'Conversion, count with states, NULL outside|conversion|ordinary|four_lanes_outside|k|count() AS c, uniqExact(number % 7) AS u|k, c, u|c DESC LIMIT 10'
    'Conversion, compiled aggregates|conversion|compiled|four_lanes_inside|k|count() AS c, sum(number) AS s|k, c, s|c DESC LIMIT 10'
    'Conversion, ascending count|conversion|ordinary|ties|k|count() AS c|k, c|c ASC LIMIT 10'
    'Conversion, String key|conversion|ordinary|inside|toString(k) AS sk|count() AS c|sk, c|c DESC LIMIT 10'
    'Conversion, LowCardinality key|conversion|ordinary|inside|toLowCardinality(toString(k)) AS lk|count() AS c|lk, c|c DESC LIMIT 10'
    'Conversion, two keys|conversion|ordinary|inside|k, k % 3 AS r|count() AS c|k, r, c|c DESC LIMIT 10'
    'Threshold merge, max, NULL inside|threshold_merge|ordinary|inside|k|max(number) AS c, uniqExact(number % 7) AS u|k, c, u|c DESC LIMIT 10'
    'Threshold merge, max, NULL outside|threshold_merge|ordinary|outside|k|max(number) AS c, uniqExact(number % 7) AS u|k, c, u|c DESC LIMIT 10'
    'Threshold merge, count, NULL inside|threshold_merge|one_stream|one_stream_inside|k|count() AS c, max(number) AS m|k, c, m|c DESC LIMIT 10'
    'Threshold merge, count, NULL outside|threshold_merge|one_stream|one_stream_outside|k|count() AS c, max(number) AS m|k, c, m|c DESC LIMIT 10'
    'Adaptive, count, NULL inside|pruning|adaptive|inside|k|count() AS c|k, c|c DESC LIMIT 10'
    'Adaptive, count, NULL outside|pruning|adaptive|outside|k|count() AS c|k, c|c DESC LIMIT 10'
    'Adaptive, uniqExact, NULL inside|pruning|adaptive|inside|k|uniqExact(number) AS c|k, c|c DESC LIMIT 10'
    'Adaptive, uniqExact, NULL outside|pruning|adaptive|outside|k|uniqExact(number) AS c|k, c|c DESC LIMIT 10'
    'Adaptive, count first, NULL inside|count_first|adaptive|inside|k|count() AS c, sum(number) AS s|k, c, s|c DESC LIMIT 10'
    'Adaptive, count first, NULL outside|count_first|adaptive|outside|k|count() AS c, sum(number) AS s|k, c, s|c DESC LIMIT 10'
    'Adaptive, String key|pruning|adaptive|inside|toString(k) AS sk|count() AS c|sk, c|c DESC LIMIT 10'
    'Adaptive, two keys|pruning|adaptive|inside|k, k % 3 AS r|count() AS c|k, r, c|c DESC LIMIT 10'
    'Adaptive, ascending count|pruning|adaptive|ties|k|count() AS c|k, c|c ASC LIMIT 10'
    'Adaptive, ascending uniqExact|pruning|adaptive|ties|k|uniqExact(number) AS c|k, c|c ASC LIMIT 10'
    'Adaptive, ascending count first|count_first|adaptive|ties|k|count() AS c, sum(number) AS s|k, c, s|c ASC LIMIT 10'
    'Adaptive, ascending, groups of one row|conversion|adaptive|single_rows|k|count() AS c|k, c|c ASC LIMIT 10'
)

queries="$settings"
for spec in "${cells[@]}"; do
    queries+=$(cell "$spec")
done

$CLICKHOUSE_LOCAL --query "$queries" | awk -F'\t' '
    NR % 2 == 1 { label = $1; counter = $2; result = $3 "\t" $4; next }
    {
        printf "%s\t%s\t%s\n", label, result, ($counter - previous[counter] > 0 ? "ran" : "did not run");
        for (i = 1; i <= 4; ++i)
            previous[i] = $i;
    }'
