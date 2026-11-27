#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# `_minmax_count_projection` builds one `min`/`max` state per part. For a `Tuple` key that state owns
# heap memory, so a state that is never destroyed is a leak. A leak reports only at process exit, so
# the queries run in a short-lived `clickhouse-local` and the leak reports in its own sanitizer log
# are counted. The count is 0 on a build without a leak sanitizer, so one reference matches every
# build type. The last `log_path` wins over the runner's, and the pipeline keeps the shell from
# printing a job status line to stderr when the sanitizer aborts.
log="${CLICKHOUSE_TMP}/05828_${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -f "$log"*

o="log_path=$log"
ASAN_OPTIONS="${ASAN_OPTIONS:+$ASAN_OPTIONS:}$o" MSAN_OPTIONS="${MSAN_OPTIONS:+$MSAN_OPTIONS:}$o" \
TSAN_OPTIONS="${TSAN_OPTIONS:+$TSAN_OPTIONS:}$o" UBSAN_OPTIONS="${UBSAN_OPTIONS:+$UBSAN_OPTIONS:}$o" \
    $CLICKHOUSE_LOCAL --query "
        CREATE TABLE tab (k Tuple(UInt64, String)) ENGINE = MergeTree ORDER BY k;
        SYSTEM STOP MERGES tab;
        INSERT INTO tab VALUES ((1, 'a')), ((5, 'b'));
        INSERT INTO tab VALUES ((3, 'c'));

        SELECT min(k), max(k), count() FROM tab SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1;

        -- The query above must be answered by the projection, otherwise a zero leak count proves nothing.
        SELECT count() > 0 FROM (EXPLAIN SELECT min(k), max(k), count() FROM tab SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1)
        WHERE explain ILIKE '%_minmax_count_projection%';
    " | cat

cat "$log"* 2>/dev/null | grep -c 'detected memory leaks' || true
rm -f "$log"*
