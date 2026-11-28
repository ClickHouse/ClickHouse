#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The results of a block of 101 `timeSeriesLastToGrid` states with 100001 grid points do not fit the memory limit, so
# reserving them throws, and every state of the block must still be destroyed. The bucket map of a state is on the heap,
# so a leaked state is reported by LeakSanitizer. A leak reports only at process exit, so the `clickhouse local` gets
# its own sanitizer log and the leak reports in it are counted: 0 on every build. The pipeline keeps the shell from
# printing a job status when the sanitizer aborts.
log="${CLICKHOUSE_TMP}/05814_${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -f "$log"*
o="log_path=$log"

ASAN_OPTIONS="${ASAN_OPTIONS:+$ASAN_OPTIONS:}$o" MSAN_OPTIONS="${MSAN_OPTIONS:+$MSAN_OPTIONS:}$o" \
TSAN_OPTIONS="${TSAN_OPTIONS:+$TSAN_OPTIONS:}$o" UBSAN_OPTIONS="${UBSAN_OPTIONS:+$UBSAN_OPTIONS:}$o" \
    ${CLICKHOUSE_LOCAL} --ignore-error --query "
        SELECT number AS k, timeSeriesLastToGrid(0, 100000, 1, 1)(toUInt32(0), toFloat64(number))
        FROM numbers(1000) GROUP BY k
        SETTINGS enable_time_series_aggregate_functions = 1, enable_adaptive_aggregator = 0, max_threads = 1,
            group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0, max_block_size = 100,
            max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0,
            max_memory_usage = 64000000
        FORMAT Null;
        SELECT 'memory limit exceeded', value > 0 FROM system.errors WHERE name = 'MEMORY_LIMIT_EXCEEDED';" 2>/dev/null | cat
echo "leaks: $(cat "$log"* 2>/dev/null | grep -c 'detected memory leaks')"
rm -f "$log"*
