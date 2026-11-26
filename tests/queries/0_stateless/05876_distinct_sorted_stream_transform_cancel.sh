#!/usr/bin/env bash
# Tags: no-parallel, no-random-merge-tree-settings
# Tag no-parallel: the test parks a query on a server-global PAUSEABLE failpoint, so a concurrent
#   instance would consume this one's pause and release the query early.
# Tag no-random-merge-tree-settings: the fixtures pin wide parts on purpose, so that a background
#   merge cannot change how the data is split into chunks.

# `DistinctSortedStreamTransform` must observe a `KILL QUERY` at every poll site instead of
# finishing the sorted chunk first: inside the hash-table lookups of an equal-key range, while
# skipping a plain (no hash table) range, in the range carried over from the previous chunk, and
# at the outer-loop throttle.
#
# The failpoint replaces timing: it parks the query inside a poll site, so a `KILL QUERY` issued
# while it is parked must be observed right there. Every cancellation bail-out logs its own
# distinct TEST-level message, and the exact sequence of those messages is the oracle. The
# expected poll site must be the one that observed the cancellation, and no other marker may be
# logged, because that would mean the transform kept working after the cancellation was seen. In
# particular, dropping the immediate return that follows `continueWithPrevRange` makes the
# transform hash the remaining range of the chunk and log `Cancelled after hashing a range`, which
# the expected marker sequence below does not allow.
#
# `--send_logs_level=test` is required because these are TEST-level messages. The server drains the
# log queue to the client before sending the exception, so the markers arrive on stderr.
#
# The fixtures use wide parts (`min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0`). For a
# compact part the reader cannot read an incomplete granule
# (`MergeTreeReaderCompact::canReadIncompleteGranules` returns false), so a small table would
# arrive as one block regardless of `max_block_size`, and a background merge of two compact parts
# would change the chunk layout. A wide part honours `max_block_size`, so the chunk boundaries are
# the same whether or not the parts were merged.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FP="distinct_sorted_stream_transform_pause"
# Wide part: the reader honours `max_block_size` even for a table that fits in one granule.
WIDE_PART="min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0"

# The runner passes `--send_logs_level=<server_logs_level>`; replace it instead of appending a
# second one, because multi-token expansion may prefer the first occurrence.
if [ -n "${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL:-}" ]; then
    CLICKHOUSE_CLIENT_TEST_LOGS=${CLICKHOUSE_CLIENT//--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}/--send_logs_level=test}
else
    CLICKHOUSE_CLIENT_TEST_LOGS="${CLICKHOUSE_CLIENT} --send_logs_level=test"
fi

failed=0

cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null ||:
    wait 2>/dev/null ||:
    for table in runs runs_with_v cont runs_multi cont_with_v; do
        $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.$table" 2>/dev/null ||:
    done
}
trap cleanup EXIT

# run_case <name> <expected markers, one per line> <query>
#
# Arms the failpoint, runs the query in the background, waits until it is parked, kills it,
# releases it, and checks that it ended with `QUERY_WAS_CANCELLED` after logging exactly the
# expected sequence of `DistinctSortedStreamTransform` cancellation markers.
run_case()
{
    local name="$1"
    local expected="$2"
    local query="$3"
    local query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_${name}"
    local err="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_${name}.err"

    if ! $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FP" 2>"$err"; then
        echo "case $name: cannot arm failpoint $FP"
        cat "$err"
        failed=1
        return
    fi

    $CLICKHOUSE_CLIENT_TEST_LOGS --query_id "$query_id" -q "$query" 2>"$err" &
    local query_pid=$!

    if ! timeout 60 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP PAUSE" 2>>"$err"; then
        echo "case $name: the query did not reach failpoint $FP"
        $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null ||:
        wait "$query_pid" 2>/dev/null ||:
        failed=1
        return
    fi

    # ASYNC: a synchronous `KILL QUERY` waits for the query to leave `system.processes`, and this
    # one is parked until `SYSTEM DISABLE FAILPOINT` below.
    kill_out=$(timeout 60 $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$query_id' ASYNC" 2>>"$err")
    if [ -z "$kill_out" ]; then
        # Nothing was cancelled, so the check below would pass without exercising anything.
        echo "case $name: KILL QUERY matched no query"
        $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null ||:
        wait "$query_pid" 2>/dev/null ||:
        failed=1
        return
    fi

    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP"
    wait "$query_pid" 2>/dev/null ||:

    if ! grep -q "QUERY_WAS_CANCELLED" "$err"; then
        echo "case $name: the query did not end with QUERY_WAS_CANCELLED"
        cat "$err"
        failed=1
        return
    fi

    actual=$(sed -n 's/^.*DistinctSortedStreamTransform: \(Cancelled.*\)$/\1/p' "$err")
    if [ "$actual" != "$expected" ]; then
        echo "case $name: unexpected cancellation markers"
        echo "expected:"
        echo "$expected"
        echo "actual:"
        echo "$actual"
        failed=1
        return
    fi

    rm -f "$err"
    echo "case $name: ok"
}

# (1) Runs of length 1: the only poll reached is the outer-loop throttle at row 4096.
run_case "single_values" \
"Cancelled at the outer-loop poll
Cancelled after the row processing loop" \
"SELECT DISTINCT number FROM numbers(10000) ORDER BY number FORMAT Null
 SETTINGS max_block_size=10000, max_threads=1, max_rows_to_read=0"

# (2) Plain runs of 5000: the batch skip helper crosses its 4096-row poll inside run k=0.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.runs;
    CREATE TABLE ${CLICKHOUSE_DATABASE}.runs (k UInt32) ENGINE = MergeTree ORDER BY k
        SETTINGS $WIDE_PART;
    INSERT INTO ${CLICKHOUSE_DATABASE}.runs SELECT intDiv(number, 5000) FROM numbers(10000);
"
run_case "long_runs" \
"Cancelled while skipping a plain range at a poll site
Cancelled after skipping a plain range
Cancelled after the row processing loop" \
"SELECT DISTINCT k FROM ${CLICKHOUSE_DATABASE}.runs ORDER BY k FORMAT Null
 SETTINGS max_block_size=10000, max_threads=1, max_rows_to_read=0"

# (3) Runs of 5000 with a non-sorted column: the hash-table loop crosses i=4096 inside run k=0,
# so the in-loop poll must be the one that observes the cancellation.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.runs_with_v;
    CREATE TABLE ${CLICKHOUSE_DATABASE}.runs_with_v (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k
        SETTINGS $WIDE_PART;
    INSERT INTO ${CLICKHOUSE_DATABASE}.runs_with_v SELECT intDiv(number, 5000), number FROM numbers(10000);
"
run_case "other_columns" \
"Cancelled while hashing a range at a poll site
Cancelled after hashing a range
Cancelled after the row processing loop" \
"SELECT DISTINCT k, v FROM ${CLICKHOUSE_DATABASE}.runs_with_v ORDER BY k FORMAT Null
 SETTINGS max_block_size=10000, max_threads=1, max_rows_to_read=0"

# (4) Plain path, second chunk: with 4000-row chunks the first chunk ends inside run k=1 without
# reaching any poll, so chunk 2 goes through `continueWithPrevRange` and the poll right after the
# skipped carried-over prefix is the first one the query can hit.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.cont;
    CREATE TABLE ${CLICKHOUSE_DATABASE}.cont (k UInt32) ENGINE = MergeTree ORDER BY k
        SETTINGS $WIDE_PART;
    INSERT INTO ${CLICKHOUSE_DATABASE}.cont SELECT intDiv(number, 3000) FROM numbers(10000);
"
run_case "continuation" \
"Cancelled at the carried-over prefix poll
Cancelled after the continuation range" \
"SELECT DISTINCT k FROM ${CLICKHOUSE_DATABASE}.cont ORDER BY k FORMAT Null
 SETTINGS max_block_size=4000, max_threads=1, max_rows_to_read=0"

# (5) Hash path with several runs: the cancellation is observed inside run k=0 of the first chunk,
# which is followed by another range, so the bail-out after the hash helper must stop that range
# from being hashed. Without the bail-out the query logs `Cancelled at the outer-loop poll` instead
# of the expected pair below, and the marker sequence check fails.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.runs_multi;
    CREATE TABLE ${CLICKHOUSE_DATABASE}.runs_multi (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k
        SETTINGS $WIDE_PART;
    INSERT INTO ${CLICKHOUSE_DATABASE}.runs_multi SELECT intDiv(number, 6000), number FROM numbers(20000);
"
run_case "other_columns_multiple_runs" \
"Cancelled while hashing a range at a poll site
Cancelled after hashing a range
Cancelled after the row processing loop" \
"SELECT DISTINCT k, v FROM ${CLICKHOUSE_DATABASE}.runs_multi ORDER BY k FORMAT Null
 SETTINGS max_block_size=10000, max_threads=1, max_rows_to_read=0"

# (6) Hash path, second chunk: 3000-row chunks keep chunk 1 below the `i & 0xFFF` poll and end it
# inside run k=1, so chunk 2 really goes through `continueWithPrevRange` and hashes the carried-over
# prefix first. Removing the immediate return that follows it hashes the rest of chunk 2 instead and
# logs `Cancelled after hashing a range`, which the expected markers reject.
$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.cont_with_v;
    CREATE TABLE ${CLICKHOUSE_DATABASE}.cont_with_v (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k
        SETTINGS $WIDE_PART;
    INSERT INTO ${CLICKHOUSE_DATABASE}.cont_with_v SELECT intDiv(number, 2000), number FROM numbers(6000);
"
run_case "other_columns_continuation" \
"Cancelled while hashing the carried-over prefix
Cancelled after the continuation range" \
"SELECT DISTINCT k, v FROM ${CLICKHOUSE_DATABASE}.cont_with_v ORDER BY k FORMAT Null
 SETTINGS max_block_size=3000, max_threads=1, max_rows_to_read=0"

[ "$failed" -eq 0 ] || exit 1
