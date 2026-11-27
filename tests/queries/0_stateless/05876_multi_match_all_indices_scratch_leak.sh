#!/usr/bin/env bash
# Tags: no-fasttest, use-vectorscan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An exception thrown while vectorscan reports a match must not leak the scratch cloned for the scan. A leak reports
# only at process exit, so the query runs in a short-lived `clickhouse-local` and the leak reports in its own
# sanitizer log are counted. The count is 0 on a build without a leak sanitizer, so one reference matches every build.
needles="[$(seq -s, -f "'a|%g'" 0 999)]"
log="${CLICKHOUSE_TMP}/05876_${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -f "$log"*

o="log_path=$log"
ASAN_OPTIONS="${ASAN_OPTIONS:+$ASAN_OPTIONS:}$o" MSAN_OPTIONS="${MSAN_OPTIONS:+$MSAN_OPTIONS:}$o" \
TSAN_OPTIONS="${TSAN_OPTIONS:+$TSAN_OPTIONS:}$o" UBSAN_OPTIONS="${UBSAN_OPTIONS:+$UBSAN_OPTIONS:}$o" \
    $CLICKHOUSE_LOCAL --query "
        -- Every needle matches, so each row gets 1000 indices.
        SELECT length(multiMatchAllIndices('a', $needles));

        -- One block needs 65536 * 1000 * 8 bytes of result, far above the limit, and the result grows only while
        -- vectorscan reports matches, so the limit is hit inside the match callback.
        SELECT multiMatchAllIndices(materialize('a'), $needles) FROM numbers(100000)
        SETTINGS max_block_size = 65536, max_threads = 1, max_memory_usage = '50Mi' FORMAT Null;
    " 2>&1 | grep -o -E '^1000$|MEMORY_LIMIT_EXCEEDED' | uniq

cat "$log"* 2>/dev/null | grep -c 'detected memory leaks' || true
rm -f "$log"*
