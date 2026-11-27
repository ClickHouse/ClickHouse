#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An adaptive aggregation that feeds `ORDER BY count() LIMIT n` combines the misses of a key into one record
# of its best row when the other aggregates select one row by an ordering column, as `argMax(x, o)` does. Such
# a record stands for the rows it combined, so the count bins of the top-K and the counting of the count-first
# merge units add those rows.
#
# Each lane brings 1024 keys, which fill and freeze the table of its producer, with 3 rows each, and 11 of
# them get 51 rows. Then 4 missed keys bring 100 consecutive rows each, in blocks of 16 rows, so each of their
# records is written while more of its rows follow. Two keys order their rows down, so the later rows only add
# to the multiplicity of the written record. The other two order them up, so a later row replaces the written
# record and takes over its rows. Last, 200 missed keys bring 4 rows each, which the pruning skips. Counting a
# record as one row would rank the 4 heavy keys below the 11 grown table keys.
#
# The top-K runs with `adaptive_aggregator_combine_arg_min_max` off and then on, and prints its rows each
# time. A line after them gives the records staged without and with the setting, and whether the merge pruned
# records with it.
#
# Everything runs in one clickhouse-local process, so the counters in `system.events` belong to these queries,
# and each query's share is the difference of the counters around it. Four UNION ALL lanes feed the four
# producers, one each, so the blocks and the frozen tables do not depend on the scheduling.

ROWS="SELECT multiIf(n < 3072, n % 1024, n < 3600, n % 11, n < 4000, 10000 + intDiv(n, 100), 20000 + intDiv(n, 4)) AS k,
    if(n >= 3600 AND n < 3800, -toInt64(n), toInt64(n)) AS o, n AS x
FROM (SELECT number % 4800 AS n FROM (
    SELECT number FROM numbers(0, 4800) UNION ALL SELECT number FROM numbers(4800, 4800)
    UNION ALL SELECT number FROM numbers(9600, 4800) UNION ALL SELECT number FROM numbers(14400, 4800)))"

queries=""
for value in 0 1; do
    queries+="
    SELECT 'top-K', ${value}, k, a, c FROM (
        SELECT k, argMax(x, o) AS a, count() AS c FROM (${ROWS}) GROUP BY k ORDER BY c DESC LIMIT 4
        SETTINGS adaptive_aggregator_combine_arg_min_max = ${value}) ORDER BY k;
    SELECT 'counters', sumIf(value, event = 'AdaptiveAggregationStagedRecords'),
        sumIf(value, event = 'AdaptiveAggregationPrunedRecords') FROM system.events;"
done

$CLICKHOUSE_LOCAL --query "
SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 16;
SET enable_adaptive_aggregator = 1, adaptive_aggregator_freeze_threshold = 1024, adaptive_aggregator_freeze_threshold_bytes = 0;
SET adaptive_aggregator_disable_thaw = 1, collect_hash_table_stats_during_aggregation = 0;
SET query_plan_enable_optimizations = 1, query_plan_aggregation_bucket_top_k = 1;
SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 1000000000;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
SET optimize_aggregation_in_order = 0, max_rows_to_group_by = 0;
${queries}
" | awk -F'\t' '
    # The counters are cumulative, so each top-K staged and pruned the difference to the line before.
    $1 == "counters" {
        staged[++runs] = $2 - previous_staged
        pruned[runs] = $3 - previous_pruned
        previous_staged = $2
        previous_pruned = $3
        if (runs == 2)
            print "staged records without and with the setting: " staged[1] ", " staged[2] "\tpruned records with the setting: " (pruned[2] > 0)
        next
    }
    { print }'
