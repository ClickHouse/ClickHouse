#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Under `group_by_overflow_mode = 'any'` the merge of the per-thread tables drops the keys a later table has and the first
# one does not. The `uniqExact` state of each dropped key owns a hash set on the heap, so a state that is not destroyed is
# reported by LeakSanitizer when `clickhouse local` exits: each case gets its own sanitizer log and its leak reports are
# counted, 0 on every build. Each thread of `numbers_mt` reads its own blocks of 1000 numbers, so the threads have disjoint
# keys and every table stops adding keys at 101.
run()
{
    local log="${CLICKHOUSE_TMP}/05817_$1_${CLICKHOUSE_TEST_UNIQUE_NAME}"
    rm -f "$log"*
    local o="log_path=$log"
    ASAN_OPTIONS="${ASAN_OPTIONS:+$ASAN_OPTIONS:}$o" MSAN_OPTIONS="${MSAN_OPTIONS:+$MSAN_OPTIONS:}$o" \
    TSAN_OPTIONS="${TSAN_OPTIONS:+$TSAN_OPTIONS:}$o" UBSAN_OPTIONS="${UBSAN_OPTIONS:+$UBSAN_OPTIONS:}$o" \
        ${CLICKHOUSE_LOCAL} --query "
            SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 1000,
                max_rows_to_group_by = 100, group_by_overflow_mode = 'any', totals_mode = 'before_having',
                group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0,
                collect_hash_table_stats_during_aggregation = 0,
                max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
            SELECT $2 AS k, uniqExact(number) FROM numbers_mt(2000000) GROUP BY k $3 FORMAT Null;
            SELECT '$1', value > 0 FROM system.events WHERE event = 'OverflowAny';" 2>/dev/null | cat
    echo "$1 leaks: $(cat "$log"* 2>/dev/null | grep -c 'detected memory leaks')"
    rm -f "$log"*
}

run number "intDiv(number, 1000)" ""
run string "toString(intDiv(number, 1000))" ""
# With an overflow row the merge drops them too: the overflow row is emitted before the tables are merged.
run totals "intDiv(number, 1000)" "WITH TOTALS"
