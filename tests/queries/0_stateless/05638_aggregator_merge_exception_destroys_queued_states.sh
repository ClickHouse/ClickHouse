#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The merge of the per-thread tables of GROUP BY detaches the states of a key found in several tables
# from the source table and queues them for a batch merge. A failpoint throws either while states are
# queued (as a memory limit does when the destination table grows) or inside the batch merge after its
# first row, and every queued state must be destroyed exactly once. The array of each `quantileExact`
# state is on the heap, so a leaked state is reported by LeakSanitizer at exit, and a state destroyed
# twice makes AddressSanitizer abort before the remaining cases print. Each case prints the count of
# injected faults so far, so a case that does not throw shows too.
#
# A leak reports only at process exit, so each `clickhouse local` gets its own sanitizer log and the
# leak reports in it are counted: 0 on every build. The log also keeps the report out of the runner's
# fatal log, and the pipeline keeps the shell from printing a job status on an abort.
run()
{
    local log="${CLICKHOUSE_TMP}/05638_$1_${CLICKHOUSE_TEST_UNIQUE_NAME}"
    rm -f "$log"*
    local o="log_path=$log"
    ASAN_OPTIONS="${ASAN_OPTIONS:+$ASAN_OPTIONS:}$o" MSAN_OPTIONS="${MSAN_OPTIONS:+$MSAN_OPTIONS:}$o" \
    TSAN_OPTIONS="${TSAN_OPTIONS:+$TSAN_OPTIONS:}$o" UBSAN_OPTIONS="${UBSAN_OPTIONS:+$UBSAN_OPTIONS:}$o" \
        ${CLICKHOUSE_LOCAL} --ignore-error --query "
            SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 1000,
                collect_hash_table_stats_during_aggregation = 0, compile_aggregate_expressions = 0,
                max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
            $2" 2>/dev/null | cat
    echo "$1 leaks: $(cat "$log"* 2>/dev/null | grep -c 'detected memory leaks')"
    rm -f "$log"*
}

# One case: name, failpoint, settings, key, aggregate functions.
query()
{
    echo "SYSTEM ENABLE FAILPOINT $2;
        SELECT k, ${5:-quantileExact(number), quantileExact(number + 1)}
        FROM (SELECT number, $4 AS k FROM numbers_mt(400000)) GROUP BY k SETTINGS $3 FORMAT Null;
        SELECT '$1', value FROM system.errors WHERE name = 'FAULT_INJECTED';"
}

single="enable_adaptive_aggregator = 0, group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0"
partition="$single, enable_parallel_single_level_merge = 1"
serial="$single, enable_parallel_single_level_merge = 0"
bucket="enable_adaptive_aggregator = 0, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 0"
collect=aggregator_merge_collect_states_throw
batch=aggregator_merge_batch_throw

# A throw after states are queued: the parallel hash-partition merge of a table with cell iteration, of a
# string table and of a table with a NULL key, the serial merge of single-level tables, the merge of a
# two-level bucket and the parallel merge of fixed hash maps.
run collect "
    $(query partition $collect "$partition" "toUInt64(number % 1000)")
    $(query partition_string $collect "$partition" "toString(number % 1000)")
    $(query partition_nullable $collect "$partition" "if(number % 10 = 0, NULL, toUInt64(number % 1000))")
    $(query serial $collect "$serial" "toUInt64(number % 1000)")
    $(query bucket $collect "$bucket" "toUInt64(number % 1000)")
    $(query fixed_map $collect "$single" "toUInt16(number % 1000)")"

# A throw inside the batch merge of the first function after its first row, in both merges, and inside
# the first nested function of a `-Tuple` combinator.
run batch "
    $(query partition_batch $batch "$partition" "toUInt64(number % 1000)")
    $(query serial_batch $batch "$serial" "toUInt64(number % 1000)")
    $(query tuple_batch $batch "$partition" "toUInt64(number % 1000)" "quantileExactTuple((number, number + 1))")"
