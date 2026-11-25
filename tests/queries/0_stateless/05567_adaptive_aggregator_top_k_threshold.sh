#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The merge units of an adaptive aggregation that feeds `ORDER BY ... LIMIT n` by a `count` or `uniqExact`
# share a threshold: the worst of the `n` best ranks that the converted units output, a floor for a descending
# order and a ceiling for an ascending one. Every later conversion drops the groups ranked worse, and so does
# a unit that counts first, as `count` among other aggregates does. An ascending order is settled once its
# threshold reaches the smallest rank that its aggregate can give a group, so the merge skips the units left.
# That rank is 0 when the aggregate can skip rows, as one of a `Nullable` argument or `uniqExactIf` can, and 1
# otherwise.
#
# The source has 10000 keys of about 20 rows each, so `LIMIT 1000` is above a unit's group count, and a
# unit's own top keeps all its groups. `x` is NULL for the keys below 100, `y` is 0 for the keys below 1000,
# and `z` is NULL below 100 and `y` above. So `count(x)`, `uniqExact(z)` and `uniqExactIf(y, k >= 100)` rank
# the keys below 100 as 0, and the latter two rank the keys from 100 to 999 as 1. Each cell compares its top
# with the one of the feature off, as a multiset of ranks and row by row against the full aggregation. Every
# query runs under a throw-mode group limit, which keeps the threshold and the settlement on, because the
# merge checks the limit against the groups it builds.
#
# The cells of `sort_cells` also check that fewer than 9000 of the 10000 groups reached the sort
# (`rows_before_limit_at_least`), as every group of an ascending order does without the threshold. A
# descending order also has the count bins of the top-K pruning, which bound a group's rank by its rows.
# Almost every key has a bin of its own here, so the bins bound a `count` exactly and drop every group below
# a `count` floor before the floor does. So the floor cell ranks by `uniqExact(number % 10)`, which is at most
# 10 while the bins hold about 20 rows per key: only the floor drops its groups, and about 4500 reach the
# sort, against about 9900 without the floor. The cells of `result_cells` check their results only: a merge
# settled at 1 would stop at the first ten groups ranked 0 or 1, most of which rank 1. `Settled at 0` checks
# in a process of its own that the merge skipped units.
# `max_threads_min_free_memory_per_thread = 0` keeps the threads whatever the free memory.

source="SELECT toUInt64(intHash64(number) % 10000) AS k, number, if(k < 100, NULL, number) AS x, if(k < 1000, 0, number) AS y,
    if(k < 100, NULL, y) AS z FROM numbers_mt(200000)"
settings="
    SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 8192;
    SET adaptive_aggregator_freeze_threshold = 128;
    SET collect_hash_table_stats_during_aggregation = 0, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
    SET query_plan_enable_optimizations = 1, query_plan_aggregation_bucket_top_k = 1, enable_adaptive_aggregator = 1;
    SET max_rows_to_group_by = 10000000, group_by_overflow_mode = 'throw';"

function top() { echo "SELECT k, $1 FROM ($source) GROUP BY k ORDER BY $2"; }

# Selects the label `$1` and whether the top by the aggregates `$2` in the order `$3` matches the feature off.
function exact()
{
    local query columns
    query=$(top "$2" "$3")
    columns="k, $(grep -oE ' AS [a-z]+' <<< "$2" | sed 's/ AS //' | paste -sd ',' -)"
    echo "SELECT '$1',
        (SELECT arraySort(groupArray(c)) FROM ($query)) = (SELECT arraySort(groupArray(c)) FROM ($query SETTINGS enable_adaptive_aggregator = 0)),
        (SELECT count() FROM ($query) WHERE ($columns) NOT IN (SELECT k, $2 FROM ($source) GROUP BY k SETTINGS enable_adaptive_aggregator = 0)) = 0;"
}

# A cell is `label|aggregates|order`.
sort_cells=(
    'Floor, uniqExact|uniqExact(number % 10) AS c|c DESC LIMIT 1000'
    'Ceiling, count|count() AS c|c ASC LIMIT 1000'
    'Ceiling, count among others|count() AS c, max(number) AS m|c ASC LIMIT 1000')
result_cells=(
    'Smallest rank 0, uniqExact of Nullable|uniqExact(z) AS c|c ASC LIMIT 10'
    'Smallest rank 0, uniqExactIf|uniqExactIf(y, k >= 100) AS c|c ASC LIMIT 10')

# One process runs every cell, then the queries of `sort_cells` again in `FORMAT JSON`, which reports the rows
# that reached the sort.
queries=""
for cell in "${sort_cells[@]}" "${result_cells[@]}"; do
    IFS='|' read -r label aggregates order <<< "$cell"
    queries+="$(exact "$label" "$aggregates" "$order") "
done
for cell in "${sort_cells[@]}"; do
    IFS='|' read -r _ aggregates order <<< "$cell"
    queries+="$(top "$aggregates" "$order") FORMAT JSON; "
done
output=$($CLICKHOUSE_LOCAL --query "$settings $queries")
paste <(head -n "${#sort_cells[@]}" <<< "$output") <(grep -oE '"rows_before_limit_at_least": [0-9]+' <<< "$output" | awk '{ print ($2 < 9000) }')
sed -n "$((${#sort_cells[@]} + 1)),$((${#sort_cells[@]} + ${#result_cells[@]}))p" <<< "$output"

$CLICKHOUSE_LOCAL --query "$settings $(exact 'Settled at 0' 'count(x) AS c' 'c ASC LIMIT 10')
    SELECT (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPrunedUnits') > 0;" | paste -sd '\t'
