#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The merge of an adaptive aggregation that feeds `ORDER BY <rank> DESC LIMIT n` skips the count bins bounded below
# its threshold, the worst of the `n` best ranks that it has output. The threshold starts from a floor that the
# producers know before the merge: the rank of a group in a producer's own table counts only what that producer
# aggregated into it, so the group's final rank is at least that, and the `n`-th largest rank of a table is one that
# `n` groups reach.
#
# Each cell aggregates two streams of 400,000 rows, one per producer. Five heavy keys take 10, 8, 6, 4 and 2 percent
# of the rows of each stream, and every other row has a key of its own. The freeze threshold is 128 keys, which a
# table passes in its first slice of 4,096 rows, so each table freezes after 4,096 rows with the heavy keys in it.
# Three rows in ten hit a frozen table, so the tables keep probing and take every row of the heavy keys: each table
# ranks them 40,000, 32,000, 24,000, 16,000 and 8,000, and each producer stages about 277,000 records of the other
# keys. With `LIMIT 3` the floor is 24,000, the third largest rank of a table. A bin without a heavy key holds a few
# records of both streams, far below the floor, so the merge skips it from its first unit on, and drains only the
# few records that share a bin with a heavy key: fewer than a thousandth of the staged ones. A merge that started
# without a floor would drain the buckets that it merges before it knows three ranks, each of them 1/256 of the
# staged records.
#
# `max_threads_min_free_memory_per_thread = 0` keeps both threads whatever the free memory, so each stream has a
# table of its own. The thresholds are pinned because the runner randomizes them, and every cell runs in its own
# clickhouse-local process, so the counters in `system.events` belong to it alone.
cell()
{
    local label=$1 rank=$2
    local keys="multiIf(number % 100 < 10, 1, number % 100 < 18, 2, number % 100 < 24, 3, number % 100 < 28, 4, number % 100 < 30, 5"
    local source="SELECT ${keys}, 1000 + number) AS k, number AS v FROM numbers(400000)
        UNION ALL SELECT ${keys}, 20000000 + number) AS k, 20000000 + number AS v FROM numbers(400000)"
    local query="SELECT k, ${rank} AS r FROM (${source}) GROUP BY k ORDER BY r DESC LIMIT 3 SETTINGS __SETTINGS__"
    local off=${query//__SETTINGS__/enable_adaptive_aggregator = 0}
    local on=${query//__SETTINGS__/enable_adaptive_aggregator = 1}
    $CLICKHOUSE_LOCAL --query "
SET max_threads = 2, max_threads_min_free_memory_per_thread = 0, max_block_size = 8192;
SET adaptive_aggregator_freeze_threshold = 128, adaptive_aggregator_freeze_threshold_bytes = 0;
SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 0;
SET collect_hash_table_stats_during_aggregation = 0, optimize_aggregation_in_order = 0;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
SET query_plan_enable_optimizations = 1, query_plan_aggregation_bucket_top_k = 1;

SELECT '${label}';
${off};
SELECT 'matches the baseline', (SELECT groupArray((k, r)) FROM (${off})) = (SELECT groupArray((k, r)) FROM (${on}));
WITH
    (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationStagedRecords') AS staged,
    (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationDrainedRecords') AS drained
SELECT 'the merge drains almost no record', staged > 500000 AND drained * 1000 < staged;
"
}

# The rank is the number of rows of a group.
cell 'count' 'count()'

# The rank is the number of distinct values of a group. Every row has a value of its own, so the ranks are the row
# counts, and a table's rank of a heavy key is the number of distinct values that its producer aggregated.
cell 'uniqExact' 'uniqExact(v)'
