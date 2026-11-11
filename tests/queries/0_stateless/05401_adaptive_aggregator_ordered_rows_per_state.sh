#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `deltaSumTimestamp` needs the rows of each state in order, which the merge of the adaptive aggregator cannot keep: it
# adds the staged rows of every thread to the same states, and a thawed table holds rows newer than the records its
# thread staged before. An aggregation with such a function takes the ordinary path, which merges the states of the
# threads with `merge`. Each lane of `UNION ALL` is its own thread, `max_threads_min_free_memory_per_thread = 0`
# keeps both threads whatever the free memory, and the tables freeze at their first key. In the
# first query the lanes hold disjoint keys, which the threads stage until they thaw. In the second, a combinator of the
# function runs over lanes that hold the same keys over disjoint ranges of time. Each result matches the same query
# with the feature off, and nothing is staged, while `sum` stages its records under the same settings. The thresholds
# are pinned because the runner randomizes them, and each part runs in its own clickhouse-local process, so the
# counters in `system.events` belong to it alone.
settings="
SET max_threads = 2, max_threads_min_free_memory_per_thread = 0, max_block_size = 1024;
SET adaptive_aggregator_freeze_threshold = 1, adaptive_aggregator_freeze_threshold_bytes = 0;
SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 100000000;
SET collect_hash_table_stats_during_aggregation = 0;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;"

disjoint_keys="SELECT sum(s), max(s) FROM (
    SELECT lane * 100000 + number % 10000 AS k, deltaSumTimestamp(number, number) AS s
    FROM (SELECT toUInt64(0) AS lane, number FROM numbers(200000) UNION ALL SELECT toUInt64(1) AS lane, number FROM numbers(200000))
    GROUP BY k SETTINGS __SETTINGS__)"
shared_keys="SELECT sum(s), max(s) FROM (
    SELECT toUInt64(number % 10000) AS k, __FUNCTION__ AS s
    FROM (SELECT number FROM numbers(200000) UNION ALL SELECT number FROM numbers(200000, 200000))
    GROUP BY k SETTINGS __SETTINGS__)"
combinator_query=${shared_keys//__FUNCTION__/deltaSumTimestampIf(number, number, number % 3 != 0)}
sum_query=${shared_keys//__FUNCTION__/sum(number)}

function matches_baseline()
{
    local query=$1
    echo "(${query//__SETTINGS__/enable_adaptive_aggregator = 0}) = (${query//__SETTINGS__/enable_adaptive_aggregator = 1})"
}

$CLICKHOUSE_LOCAL --query "
$settings
SELECT 'disjoint keys of thawing threads', $(matches_baseline "$disjoint_keys");
SELECT 'combinator over shared keys', $(matches_baseline "$combinator_query");
SELECT 'nothing staged',
    (SELECT count() FROM system.events WHERE event IN ('AdaptiveAggregationStagedRecords', 'AdaptiveAggregationThaws')) = 0;
"

$CLICKHOUSE_LOCAL --query "
$settings
${sum_query//__SETTINGS__/enable_adaptive_aggregator = 1} FORMAT Null;
SELECT 'sum stages its records',
    (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationStagedRecords') > 0;
"
