#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The merge units of an adaptive aggregation that feeds `ORDER BY ... LIMIT n` by a `count` or `uniqExact`
# share a threshold: the worst of the `n` best ranks that the converted units output, a floor for a descending
# order and a ceiling for an ascending one. Every later conversion drops the groups ranked worse, and so does
# a unit that counts first, as `count` among other aggregates does. An ascending order whose threshold reaches
# the smallest possible rank is settled, so the merge skips the units left.
#
# The source has 10000 keys of about 20 rows each, so with `LIMIT 1000` above a unit's group count only the
# threshold drops groups. Its keys below 100 have only NULL values of `x`, so `count(x)` ranks them 0, and an
# ascending order must settle on them, not on a count of 1. Each cell compares its top with the one of the
# feature off, as a multiset of ranks and row by row against the full aggregation. The conversion cells then
# check that fewer rows than groups reached the sort (`rows_before_limit_at_least`), as do the cells that count
# first, and `Settled at 0` that its counter rose, in its own process. The first call runs under a throw-mode group
# limit, which turns off the count bins that would drop most groups first. The limit needs every group
# counted, so it turns the settlement off: the last query must fail, although its first units would settle it.
# `max_threads_min_free_memory_per_thread = 0` keeps the threads whatever the free memory.

source="SELECT toUInt64(intHash64(number) % 10000) AS k, number, if(k < 100, NULL, number) AS x FROM numbers_mt(200000)"
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

output=$($CLICKHOUSE_LOCAL --query "$settings SET $limit;
    $(exact 'Floor, count' 'count() AS c' 'c DESC LIMIT 1000') $(exact 'Floor, uniqExact' 'uniqExact(number) AS c' 'c DESC LIMIT 1000')
    $(exact 'Ceiling, count' 'count() AS c' 'c ASC LIMIT 1000')
    $(exact 'Floor, count among others' 'count() AS c, max(number) AS m' 'c DESC LIMIT 1000') $(exact 'Ceiling, count among others' 'count() AS c, max(number) AS m' 'c ASC LIMIT 1000')
    $(top 'count() AS c' 'c DESC LIMIT 1000') FORMAT JSON; $(top 'uniqExact(number) AS c' 'c DESC LIMIT 1000') FORMAT JSON;
    $(top 'count() AS c' 'c ASC LIMIT 1000') FORMAT JSON;
    $(top 'count() AS c, max(number) AS m' 'c DESC LIMIT 1000') FORMAT JSON; $(top 'count() AS c, max(number) AS m' 'c ASC LIMIT 1000') FORMAT JSON;")
paste <(head -n 5 <<< "$output") <(grep -oE '"rows_before_limit_at_least": [0-9]+' <<< "$output" | awk '{ print ($2 < 10000) }')

function check()
{
    $CLICKHOUSE_LOCAL --query "$settings SET $2; $(exact "$1" "$3" "$4")
        SELECT (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = '$5') > 0;" | paste -sd '\t'
}

check 'Settled at 0' 'max_threads = 4' 'count(x) AS c' 'c ASC LIMIT 10' AdaptiveAggregationPrunedUnits

$CLICKHOUSE_LOCAL --query "$settings $(exact 'Settled at 0' 'count(x) AS c' 'c ASC LIMIT 10')
    SELECT (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPrunedUnits') > 0;" | paste -sd '\t'
