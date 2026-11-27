#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The adaptive aggregator keeps the rows of the record of a row selection that counts rows in one byte, so such a
# record takes at most 255 rows, and a row that would take it past them starts a record of its own. The keys here
# have 600 rows each:
# - Two keys take turns, so their rows combine through the tails of their partitions, which stay open. The blocks
#   of 1000 rows cut the keys, so a record fills while it is pending and after the writer appended it.
# - The keys of the first 4096 rows of a lane are their own, so the tails close, and the keys after them come in
#   runs, which the closed tails combine as repeats of the previous miss.
# Each shape prints whether the results of `argMax` with `count` equal those of the ordinary aggregation, whether
# the count staged more records than the same selection without it, which only the full records explain, and how
# many times the tails closed.
#
# Everything runs in one clickhouse-local process, so the counters in `system.events` belong to these queries, and
# each query's share is the difference of the counters around it. Four UNION ALL lanes feed the four producers, one
# each, and every local table freezes at its first key. `o` has no ties, so every group has one best row and the
# results are exact.

TURNS_ROWS="SELECT number AS x, (number % 2) * 1000000 + intDiv(number, 1200) AS k, (number * 7919) % 1000003 AS o FROM (
    SELECT number FROM numbers(0, 24000) UNION ALL SELECT number FROM numbers(24000, 24000)
    UNION ALL SELECT number FROM numbers(48000, 24000) UNION ALL SELECT number FROM numbers(72000, 24000))"
RUNS_ROWS="SELECT number AS x, if(number % 16384 < 4096, number, 1000000000 + intDiv(number, 600)) AS k,
    (number * 7919) % 1000003 AS o FROM (
    SELECT number FROM numbers(0, 16384) UNION ALL SELECT number FROM numbers(16384, 16384)
    UNION ALL SELECT number FROM numbers(32768, 16384) UNION ALL SELECT number FROM numbers(49152, 16384))"

queries=""
counters="SELECT sumIf(value, event = 'AdaptiveAggregationStagedRecords'), sumIf(value, event = 'AdaptiveAggregationTailClosures')
    FROM system.events"
function add_shape()
{
    local label=$1 rows=$2
    local query="SELECT k, argMax(x, o), max(o), count() FROM (${rows}) GROUP BY k"
    queries+="
    SELECT 'result', '${label}', count(), sum(cityHash64(*)) FROM (${query} SETTINGS enable_adaptive_aggregator = 0);
    SELECT 'result', '${label}', count(), sum(cityHash64(*)) FROM (${query} SETTINGS enable_adaptive_aggregator = 1);
    SELECT 'with count', '${label}', * FROM (${counters});
    SELECT k, argMax(x, o), max(o) FROM (${rows}) GROUP BY k SETTINGS enable_adaptive_aggregator = 1 FORMAT Null;
    SELECT 'without count', '${label}', * FROM (${counters});"
}

add_shape "keys taking turns" "${TURNS_ROWS}"
add_shape "runs after the tails closed" "${RUNS_ROWS}"

$CLICKHOUSE_LOCAL --query "
SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 1000;
SET adaptive_aggregator_freeze_threshold = 1, adaptive_aggregator_freeze_threshold_bytes = 0;
SET adaptive_aggregator_disable_thaw = 1, collect_hash_table_stats_during_aggregation = 0;
SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 1000000000;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
SET optimize_aggregation_in_order = 0;
${queries}
" | awk -F'\t' '
    # A shape prints its result with the adaptive aggregator off, then on, and the cumulative counters after the
    # selection with the count and after the one without it.
    $1 == "result" { result[$2, ++results[$2]] = $3 "\t" $4; next }
    $1 == "with count" {
        staged_with_count = $3 - staged
        closures_with_count = $4 - closures
        staged = $3
        closures = $4
        next
    }
    $1 == "without count" {
        print $2 "\tresults match: " (result[$2, 1] == result[$2, 2]) "\tmore records with count: " (staged_with_count > $3 - staged) "\tclosures: " closures_with_count
        staged = $3
        closures = $4
    }'
