#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A run of the adaptive aggregator whose producers thaw stores a verdict in the hash-table statistics, and the
# verdict keeps the later runs of the query on the ordinary path. A lone producer thaws by a lower bound than one
# of several, so the statistics keep its verdict apart: after a run with one stream thawed, the same query with
# several streams still freezes its tables, and a later run with one stream does not.
#
# `numbers_mt` gives the query a stream per thread, and the statistics of a query do not depend on its threads,
# so the cells run one query with one and with four threads. Its keys repeat 8 times. That thaws a lone producer
# and no producer of several, however the blocks fall between the threads, and with four threads some producer
# always reads enough keys to freeze. The counters of a cell are the change of `system.events` over it, so all
# cells run in one clickhouse-local process, which also has hash-table statistics of its own.

settings="max_block_size = 8192, max_threads_min_free_memory_per_thread = 0, compile_aggregate_expressions = 0,
    optimize_aggregation_in_order = 0, collect_hash_table_stats_during_aggregation = 1,
    group_by_two_level_threshold = 100000, group_by_two_level_threshold_bytes = 50000000,
    max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0,
    adaptive_aggregator_freeze_threshold = 16384, adaptive_aggregator_freeze_threshold_bytes = 4194304,
    enable_adaptive_aggregator = 1"

# Prints the two queries of a cell: the aggregation with the given threads, and the counters so far.
function cell()
{
    local name=$1 threads=$2
    echo "SELECT '$name', count(), sum(c)
        FROM (SELECT toUInt64(number % 40000) AS k, count() AS c FROM numbers_mt(320000) GROUP BY k)
        SETTINGS max_threads = $threads;"
    echo "SELECT sumIf(value, event = 'AdaptiveAggregationLocalFreezes'), sumIf(value, event = 'AdaptiveAggregationThaws')
        FROM system.events;"
}

function queries()
{
    echo "SET $settings;"
    cell 'one stream' 1
    cell 'four streams' 4
    cell 'one stream again' 1
    cell 'four streams again' 4
}

$CLICKHOUSE_LOCAL --query "$(queries)" | awk -F'\t' '
    NR % 2 == 1 { result = $0; next }
    {
        printf "%s\t%s\t%s\n", result, ($1 > freezes ? "froze" : "did not freeze"), ($2 > thaws ? "thawed" : "did not thaw");
        freezes = $1; thaws = $2;
    }'
