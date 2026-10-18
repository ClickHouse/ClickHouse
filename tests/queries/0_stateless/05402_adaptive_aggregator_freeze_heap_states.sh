#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The byte bound of the adaptive aggregator's freeze measures a table's own tracked memory, which counts the heap
# memory that its states own. Fifty groups of `uniqExact` states stay far below the key bound, but their sets grow
# with every block: they freeze the tables once they outgrow the byte bound, and the tables keep learning to the end
# under a byte bound out of their reach. Each result matches the same query with the feature off. The thresholds are
# pinned because the runner randomizes them, and each part runs in its own clickhouse-local process, so the counters
# in `system.events` belong to it alone.
function run_part()
{
    local label=$1 freeze_bytes=$2
    local query="SELECT count(), sum(u) FROM (
        SELECT toUInt64(number % 50) AS k, uniqExact(number) AS u FROM numbers_mt(2000000) GROUP BY k SETTINGS __SETTINGS__)"
    local off=${query//__SETTINGS__/enable_adaptive_aggregator = 0}
    local on=${query//__SETTINGS__/enable_adaptive_aggregator = 1}
    echo "$label"
    $CLICKHOUSE_LOCAL --query "
    SET max_threads = 4, max_block_size = 8192;
    SET adaptive_aggregator_freeze_threshold = 16384, adaptive_aggregator_freeze_threshold_bytes = ${freeze_bytes};
    SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 500000000;
    SET collect_hash_table_stats_during_aggregation = 0;
    SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

    SELECT 'matches the baseline', (${off}) = (${on});
    SELECT 'tables frozen', (SELECT count() FROM system.events WHERE event = 'AdaptiveAggregationLocalFreezes') > 0;
    "
}

run_part 'Sets over the byte bound' 1000000
run_part 'Sets under the byte bound' 1000000000
