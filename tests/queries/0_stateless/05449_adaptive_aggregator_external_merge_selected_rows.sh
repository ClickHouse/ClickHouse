#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Half of the rows repeat a few hundred hot keys, whose `groupArray` states grow in the frozen tables, and the other
# half are distinct cold keys, which are staged. Over the external-aggregation threshold the tables are written to
# disk, and the merge goes external: the staged records join it as chunks of aggregate states. The scan already
# counted those rows as input, so the query reads 200000 rows of 8 bytes, as without the adaptive aggregator. The
# thresholds are pinned because the runner randomizes them, and the query runs in a clickhouse-local process of its
# own, so the counters in `system.events` belong to it alone.
$CLICKHOUSE_LOCAL --query "
    SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 8192;
    SET enable_adaptive_aggregator = 1, adaptive_aggregator_disable_thaw = 1, collect_hash_table_stats_during_aggregation = 0;
    SET adaptive_aggregator_freeze_threshold = 1000, adaptive_aggregator_freeze_threshold_bytes = 10000;
    SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 500000000;
    SET max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0;
    SELECT count(), sum(length(a)) FROM (
        SELECT if(number % 2 = 0, number % 500, number) AS g, groupArray(number) AS a FROM numbers_mt(200000) GROUP BY g);
    SELECT
        sumIf(value, event = 'SelectedRows'),
        sumIf(value, event = 'SelectedBytes'),
        sumIf(value, event = 'AdaptiveAggregationFrozenTableSpills') > 0,
        sumIf(value, event = 'AdaptiveAggregationStagedRecords') > 0
    FROM system.events;
"
