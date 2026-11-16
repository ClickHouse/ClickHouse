#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Once the last bucket of the merge of an adaptive aggregation has merged, the merge frees what the producers left:
# their tables with the cell indexes and arenas, their staging buffers and their count bins. With 32 producers or
# more it spreads the work over jobs of 16 producers each, on the aggregator's thread pool. The chunks that the merge
# has already converted are still on their way to the output then, so they must not depend on anything that is
# freed.
#
# Each cell aggregates 40 streams of 20,000 rows, one per producer, which makes two jobs. In every stream a quarter of
# the rows take one of 99 keys, whose counts grow with the key, and every other row has a key of its own. The freeze
# threshold is 128 keys, which a table passes in its first slice of 4,096 rows, so each table freezes with 63 of the
# shared keys in it. The merge takes those keys from the cells of 40 tables, the other shared keys from the records
# of 40 producers, and every other key from one staged record. The result matches the same query with the feature
# off.
#
# `max_threads_min_free_memory_per_thread = 0` keeps the 40 threads whatever the free memory, so each stream has a
# table of its own. The thresholds are pinned because the runner randomizes them, and every cell runs in its own
# clickhouse-local process, so the counters in `system.events` belong to it alone.
source=""
for i in $(seq 1 40); do
    [ -n "$source" ] && source+=" UNION ALL "
    source+="SELECT if(number % 4 = 0, toUInt64(sqrt(number % 10000)), ${i}000000 + number) AS k, ${i}000000 + number AS v FROM numbers(20000)"
done

cell()
{
    local label=$1 query=$2
    local off=${query//__SETTINGS__/enable_adaptive_aggregator = 0}
    local on=${query//__SETTINGS__/enable_adaptive_aggregator = 1}
    $CLICKHOUSE_LOCAL --query "
SET max_threads = 40, max_threads_min_free_memory_per_thread = 0, max_block_size = 8192;
SET adaptive_aggregator_freeze_threshold = 128, adaptive_aggregator_freeze_threshold_bytes = 0;
SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 0;
SET collect_hash_table_stats_during_aggregation = 0, optimize_aggregation_in_order = 0;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
SET query_plan_enable_optimizations = 1, query_plan_aggregation_bucket_top_k = 1;

SELECT '${label}', (${off}) = (${on});
WITH
    (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationLocalFreezes') AS freezes,
    (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationMergeUnits') AS units
SELECT 'every producer froze and the buckets merged', freezes = 40 AND units > 0;
"
}

# The states of `uniqExact` own memory: the merge takes them out of the cells of the producers' tables, so the
# tables are freed without their states.
cell 'states with destructors' "SELECT (count(), sum(u), sum(cityHash64(k, u))) FROM (
    SELECT k, uniqExact(v) AS u FROM (${source}) GROUP BY k SETTINGS __SETTINGS__)"

# A top-K by the count prunes with the count bins of the producers, which the merge frees as well. The counts are
# compared as a multiset, which ties cannot reorder.
cell 'top-K with count bins' "SELECT arraySort(groupArray(c)) FROM (
    SELECT k, count() AS c FROM (${source}) GROUP BY k ORDER BY c DESC LIMIT 10 SETTINGS __SETTINGS__)"
