#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A frozen producer of the adaptive aggregator combines a missed row into the last record of its partition, the
# partition's tail, when that record holds the row's key. The rows of two keys alternate here, so a key's repeats
# reach a tail that is not the batch's last descriptor. Blocks of 12 rows cut the 16 rows of a pair of keys, so the
# repeats also reach tails written in an earlier block: a count adds to the record in place, and a row selection
# keeps the record of a worse row and replaces it with a better one. The shapes cover key-only, count and
# row-selection records, and an ordering column in `Sparse` serialization, which the staging reads row by row when
# the selection moved a record to a later row. Each shape prints whether its results equal those of the ordinary
# aggregation and whether rows combined.
#
# Everything runs in one clickhouse-local process, so the combined-row counter in `system.events` belongs to these
# queries, and each query's share is the difference of the counter around it. Four UNION ALL lanes feed the four
# producers, one each. `o` has no ties, so every group has one best row and the results are exact.

ROWS="SELECT number AS x, (number % 2) * 1000000 + intDiv(number, 16) AS k, (number * 7919) % 1000003 AS o FROM (
    SELECT number FROM numbers(0, 40000) UNION ALL SELECT number FROM numbers(40000, 40000)
    UNION ALL SELECT number FROM numbers(80000, 40000) UNION ALL SELECT number FROM numbers(120000, 40000))"

queries=""
counter="SELECT sum(value) FROM system.events WHERE event = 'AdaptiveAggregationCombinedRows'"
function add_shape()
{
    local label=$1 query=$2
    queries+="
    SELECT 'result', '${label}', count(), sum(cityHash64(*)) FROM (${query//__ADAPTIVE__/0});
    SELECT 'result', '${label}', count(), sum(cityHash64(*)) FROM (${query//__ADAPTIVE__/1});
    SELECT 'combined', '${label}', (${counter});"
}

add_shape "key-only" "SELECT k FROM (${ROWS}) GROUP BY k SETTINGS enable_adaptive_aggregator = __ADAPTIVE__"
add_shape "count" "SELECT k, count() FROM (${ROWS}) GROUP BY k SETTINGS enable_adaptive_aggregator = __ADAPTIVE__"
add_shape "selection with count" "SELECT k, argMax(x, o), max(o), count() FROM (${ROWS}) GROUP BY k
    SETTINGS enable_adaptive_aggregator = __ADAPTIVE__"
add_shape "selection without count" "SELECT k, argMin(x, o), min(o) FROM (${ROWS}) GROUP BY k
    SETTINGS enable_adaptive_aggregator = __ADAPTIVE__"
# `max(ts)` takes the `Sparse` column of the part as it is. The table is small, so it asks for concurrent reads, which
# give every producer a stream.
add_shape "Sparse ordering column" "SELECT k, max(ts), count() FROM sparse GROUP BY k
    SETTINGS enable_adaptive_aggregator = __ADAPTIVE__, merge_tree_min_rows_for_concurrent_read = 1,
    merge_tree_min_bytes_for_concurrent_read = 1"

$CLICKHOUSE_LOCAL --query "
SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 12;
SET adaptive_aggregator_freeze_threshold = 1024, adaptive_aggregator_freeze_threshold_bytes = 0;
SET adaptive_aggregator_disable_thaw = 1, collect_hash_table_stats_during_aggregation = 0;
SET group_by_two_level_threshold = 100000000, group_by_two_level_threshold_bytes = 1000000000;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
SET optimize_aggregation_in_order = 0;
CREATE TABLE sparse (k UInt64, ts UInt64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5, index_granularity = 8192;
INSERT INTO sparse SELECT (number % 2) * 1000000 + intDiv(number, 16), if(number % 3 = 0, number, 0) FROM numbers(160000);
OPTIMIZE TABLE sparse FINAL;
SELECT 'check', 'serialization of ts', serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 'sparse' AND column = 'ts' AND active;
${queries}
" | awk -F'\t' '
    # A shape prints its result with the adaptive aggregator off, then on, and the cumulative combined-row counter.
    $1 == "result" { result[$2, ++results[$2]] = $3 "\t" $4; next }
    $1 == "combined" {
        print $2 "\tresults match: " (result[$2, 1] == result[$2, 2]) "\tcombined: " ($3 - previous > 0)
        previous = $3
        next
    }
    $1 == "check" { print $2 "\t" $3 }'
