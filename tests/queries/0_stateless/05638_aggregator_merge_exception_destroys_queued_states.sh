#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The merge of the per-thread tables of GROUP BY detaches the states of a key found in several tables
# from the source table and queues them for a batch merge. A failpoint throws either while states are
# queued (as a memory limit does when the destination table grows) or inside the batch merge, and every
# queued state must be destroyed exactly once. The array of each `quantileExact` state is on the heap, so
# a leaked state makes LeakSanitizer abort `clickhouse local` at exit, and a state destroyed twice makes
# AddressSanitizer abort it.
asan=$(${CLICKHOUSE_LOCAL} --query "SELECT count() FROM system.build_options
    WHERE name = 'CXX_FLAGS' AND position('sanitize=address' IN value)")

common="SET max_threads = 4; SET max_threads_min_free_memory_per_thread = 0; SET max_block_size = 1000;
    SET collect_hash_table_stats_during_aggregation = 0;
    SET max_bytes_before_external_group_by = 0; SET max_bytes_ratio_before_external_group_by = 0;
    SET compile_aggregate_expressions = 0;"
single="SET enable_adaptive_aggregator = 0; SET group_by_two_level_threshold = 0; SET group_by_two_level_threshold_bytes = 0;"

run()
{
    out=$(${CLICKHOUSE_LOCAL} --ignore-error --query "
        SYSTEM ENABLE FAILPOINT $2;
        ${common} $3
        SELECT k, quantileExact(number), quantileExact(number + 1)
        FROM (SELECT number, $4 AS k FROM numbers_mt(400000)) GROUP BY k FORMAT Null;" 2>&1)
    rc=$?
    if [ "$asan" = "1" ] && { [ "$rc" != "0" ] || echo "$out" | grep -q "Sanitizer"; }; then
        echo "$1: FAILED rc=$rc"
    else
        echo "$1: $(echo "$out" | grep -o -m1 'FAULT_INJECTED')"
    fi
}

collect=aggregator_merge_collect_states_throw
batch=aggregator_merge_batch_throw

# A throw after states are queued, in the parallel hash-partition merge, for a table with cell iteration and for a string table.
run partition $collect "${single} SET enable_parallel_single_level_merge = 1;" "toUInt64(number % 1000)"
run partition_string $collect "${single} SET enable_parallel_single_level_merge = 1;" "toString(number % 1000)"
# The serial merge of single-level tables, the merge of a two-level bucket, the parallel merge of fixed hash maps.
run serial $collect "${single} SET enable_parallel_single_level_merge = 0;" "toUInt64(number % 1000)"
run bucket $collect "SET enable_adaptive_aggregator = 0; SET group_by_two_level_threshold = 1; SET group_by_two_level_threshold_bytes = 0;" "toUInt64(number % 1000)"
run fixed_map $collect "${single}" "toUInt16(number % 1000)"
# The default settings reach the partition merge too.
run nullable_default $collect "" "toNullable(toUInt64(number % 1000))"
# A throw inside the batch merge, after the first function merged and destroyed its states, in both merges.
run partition_batch $batch "${single} SET enable_parallel_single_level_merge = 1;" "toUInt64(number % 1000)"
run serial_batch $batch "${single} SET enable_parallel_single_level_merge = 0;" "toUInt64(number % 1000)"
