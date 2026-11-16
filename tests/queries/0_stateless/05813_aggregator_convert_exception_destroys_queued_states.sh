#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The final conversion of a GROUP BY table queues the states of a block's keys and marks their cells as moved before
# it inserts the results of the block. A failpoint throws either while states are queued (as a memory limit does when
# a key column grows) or inside the insertion of a block's results, and every state must be destroyed exactly once.
# The array of each `quantileExact` state is on the heap, so a leaked state is reported by LeakSanitizer at exit, and a
# state destroyed twice makes AddressSanitizer abort before the remaining cases print. Each case prints the count of
# injected faults so far, so a case that does not throw shows too.
#
# A leak reports only at process exit, so the `clickhouse local` gets its own sanitizer log and the leak reports in it
# are counted: 0 on every build. The log also keeps the report out of the runner's fatal log, and the pipeline keeps
# the shell from printing a job status on an abort.
log="${CLICKHOUSE_TMP}/05813_${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -f "$log"*
o="log_path=$log"

# One case: name, failpoint, settings, key, HAVING clause.
query()
{
    echo "SYSTEM ENABLE FAILPOINT $2;
        SELECT k, quantileExact(number), quantileExact(number + 1)
        FROM (SELECT number, $4 AS k FROM numbers_mt(400000)) GROUP BY k $5 SETTINGS $3 FORMAT Null;
        SELECT '$1', value FROM system.errors WHERE name = 'FAULT_INJECTED';"
}

single="enable_adaptive_aggregator = 0, group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0"
partition="$single, enable_parallel_single_level_merge = 1"
serial="$single, enable_parallel_single_level_merge = 0"
bucket="enable_adaptive_aggregator = 0, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 0"
queued=aggregator_convert_queued_states_throw
results=aggregator_convert_insert_results_throw

# A throw while states are queued: the conversion after the parallel hash-partition merge (one block), after the
# serial merge (blocks of `max_block_size`), of a two-level bucket, and of a bucket with the `HAVING count()`
# pre-filter. Then a throw inside the insertion of the first full block of a serial conversion.
ASAN_OPTIONS="${ASAN_OPTIONS:+$ASAN_OPTIONS:}$o" MSAN_OPTIONS="${MSAN_OPTIONS:+$MSAN_OPTIONS:}$o" \
TSAN_OPTIONS="${TSAN_OPTIONS:+$TSAN_OPTIONS:}$o" UBSAN_OPTIONS="${UBSAN_OPTIONS:+$UBSAN_OPTIONS:}$o" \
    ${CLICKHOUSE_LOCAL} --ignore-error --query "
        SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 1000,
            collect_hash_table_stats_during_aggregation = 0, compile_aggregate_expressions = 0,
            max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
        $(query partition $queued "$partition" "toUInt64(number % 1000)")
        $(query serial $queued "$serial" "toUInt64(number % 1000)")
        $(query bucket $queued "$bucket" "toUInt64(number % 1000)")
        $(query bucket_having $queued "$bucket" "toUInt64(number % 1000)" "HAVING count() > 1")
        $(query serial_results $results "$serial" "toUInt64(number % 5000)")" 2>/dev/null | cat
echo "leaks: $(cat "$log"* 2>/dev/null | grep -c 'detected memory leaks')"
rm -f "$log"*
