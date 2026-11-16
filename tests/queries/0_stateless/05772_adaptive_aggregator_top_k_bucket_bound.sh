#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The merge of an adaptive aggregation that feeds `ORDER BY <rank> DESC LIMIT n` skips a bucket whole, without summing
# the count bins of the bucket, when no group of the bucket can reach the merge's threshold: the sum of the producers'
# largest bins in the bucket bounds every group of it. Two things leave a bucket without that bound, and the merge
# must then look at its bins: a bin that took more rows than its 16-bit counter holds, and the table of a producer
# that never froze, whose groups the merge counts only while it merges their bucket.
#
# In every cell the heaviest key is one that the largest bins alone would bound too low. The merge's threshold starts
# from the floor that the producers' tables prove, here the count of the heaviest key in the table that holds it, so
# a bucket bounded by the largest bins alone would be skipped with that key in it. Eight more keys with 50,000 or
# 100,000 rows each sit in buckets that the bound does skip. The freeze threshold is 128 keys, which a table passes
# in its first slice of 4,096 rows, so the tables freeze with the heavy keys in them. Four rows in ten or more hit a
# frozen table, so the tables keep probing and take every row of the heavy keys. Every cell compares the top with the
# same query with the feature off.
#
# `max_threads_min_free_memory_per_thread = 0` keeps both threads whatever the free memory, so each stream has a
# table of its own. The thresholds are pinned because the runner randomizes them, and every cell runs in its own
# clickhouse-local process, so the counters in `system.events` belong to it alone.
cell()
{
    local label=$1 rank=$2 source=$3
    local query="SELECT k, ${rank} AS r FROM (${source}) GROUP BY k ORDER BY r DESC LIMIT 1 SETTINGS __SETTINGS__"
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
SELECT 'the merge skipped records',
    (SELECT coalesce(sum(value), 0) FROM system.events WHERE event = 'AdaptiveAggregationPrunedRecords') > 0;
"
}

# Key 1 takes 200,000 rows of the first stream only, more than a bin counts, so its bin saturates at 65,535. The keys 4
# to 11 take 50,000 rows of each stream, 100,000 in all, which their bins count exactly.
saturating="SELECT multiIf(number % 100 < 20, 1, number % 100 < 60, intDiv(number % 100, 5), 1000 + number) AS k
    FROM numbers(1000000)
    UNION ALL
    SELECT multiIf(number % 100 < 40, intDiv(number % 100, 5) + 4, 20000000 + number) AS k
    FROM numbers(1000000)"
cell 'saturated bin' 'count()' "$saturating"

# The second stream holds key 1 alone, with 300,000 rows, so its table never freezes and has no bins. The keys 4 to 11
# take 50,000 rows of the first stream each. With a count the table comes without its largest counts, which leaves
# every bucket unbounded. With `uniqExact`, whose states are heavy, the producer measures the largest count of each
# bucket of its table, which the bound of the bucket then includes.
never_frozen="SELECT multiIf(number % 100 < 40, intDiv(number % 100, 5) + 4, 1000 + number) AS k, number AS v
    FROM numbers(1000000)
    UNION ALL
    SELECT toUInt64(1) AS k, 20000000 + number AS v FROM numbers(300000)"
cell 'table that never froze, count' 'count()' "$never_frozen"
cell 'table that never froze, uniqExact' 'uniqExact(v)' "$never_frozen"
