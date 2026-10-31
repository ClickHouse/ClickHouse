#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A frozen table of the adaptive aggregator thaws when its staged records keep repeating the same keys. The thaw guard
# estimates the number of distinct staged keys from the staged hashes that fall into a sample of one in 256, so a
# stream of a few repeated keys can leave the sample empty; an empty sample counts as one sampled key, so such a
# stream thaws too. In each lane the first block holds 4096 distinct keys, which reach the freeze threshold, so the
# table freezes at the end of that block; the next 200000 rows all carry one more key, whose hash falls outside the
# sample, so the lane stages only that key. Every frozen table must thaw, after about 65536 staged records, and the
# result must match the ordinary aggregation. Each lane is a single stream, so it feeds a producer of its own. The
# thresholds are pinned, the hash-table statistics are off so that no remembered verdict decides, and the counters in
# `system.events` belong to this clickhouse-local process alone.
lane="SELECT if(number < 4096, number, toUInt64(1000000)) AS k, number AS v FROM numbers(204096)"
query="SELECT count(), sum(k), sum(s) FROM (
    SELECT k, sum(v) AS s FROM ($lane UNION ALL $lane UNION ALL $lane UNION ALL $lane) GROUP BY k SETTINGS __SETTINGS__)"

$CLICKHOUSE_LOCAL --query "
SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 4096;
SET adaptive_aggregator_freeze_threshold = 4096, adaptive_aggregator_freeze_threshold_bytes = 0;
SET adaptive_aggregator_disable_thaw = 0;
SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 500000000;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
SET collect_hash_table_stats_during_aggregation = 0, optimize_aggregation_in_order = 0;

SELECT 'matches the baseline',
    (${query//__SETTINGS__/enable_adaptive_aggregator = 1}) = (${query//__SETTINGS__/enable_adaptive_aggregator = 0});
SELECT 'every frozen table thawed', freezes > 0 AND thaws = freezes FROM
(
    SELECT
        sumIf(value, event = 'AdaptiveAggregationLocalFreezes') AS freezes,
        sumIf(value, event = 'AdaptiveAggregationThaws') AS thaws
    FROM system.events
);
"
