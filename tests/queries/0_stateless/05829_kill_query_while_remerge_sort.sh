#!/usr/bin/env bash
# Tags: no-parallel (process-global failpoints), no-fasttest (bounded waits),
# no-random-settings (the pause needs the settings below, which clickhouse-test randomizes).

# Regression test for cancellation of an ORDER BY ... LIMIT query while its accumulated
# sorted blocks are re-merged in memory.

# `remerge` and `MergeSorter::read` pause on a failpoint right before polling
# `CurrentThread::checkIfNotCancelled`, which anchors the test inside the loop.

# The failpoints are `PAUSEABLE`: `NOTIFY` releases a parked thread without disarming, and
# `finalizeExecution` throws `QUERY_WAS_CANCELLED` even when the whole pass completed.

# Each arm therefore asserts the failpoint never pauses again; the positive control below
# proves that second pause is reachable, so the assertion is not vacuous.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FP_REMERGE=merge_sorting_transform_remerge_pause
FP_MERGE=merge_sorter_cancel_pause
QID_PREFIX="05829_${CLICKHOUSE_DATABASE}"

# `remerge` needs `limit * 2 < sum_rows_in_blocks`: `LIMIT 500000` with 65536-row blocks
# gives 16 blocks per pass; ratio 0 and `max_threads = 1` keep the passes deterministic.
QUERY="SELECT count(), sum(number), sum(cityHash64(number)) FROM (SELECT number, sipHash64(number) AS k FROM numbers(3000000) ORDER BY k LIMIT 500000)"
SETTINGS="max_threads = 1, max_block_size = 65536, remerge_sort_lowered_memory_bytes_ratio = 0"
REMERGE_BYTES=8000000

function cleanup()
{
    # A leaked failpoint would park unrelated work, and DISABLE releases a parked thread,
    # so it must run on every exit path.
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP_REMERGE" 2>/dev/null ||:
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP_MERGE" 2>/dev/null ||:
    $CLICKHOUSE_CLIENT -q "
        KILL QUERY WHERE (query_id LIKE '${QID_PREFIX}%' OR initial_query_id LIKE '${QID_PREFIX}%')
        AND query NOT LIKE '%KILL QUERY%' FORMAT Null" 2>/dev/null ||:
    wait 2>/dev/null ||:
}
trap cleanup EXIT

function wait_for_pause()
{
    local fp="$1"
    local message="$2"
    if ! timeout 30 $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $fp PAUSE"; then
        echo "FAIL: $message"
        exit 1
    fi
}

# The query must really go through `MergeSortingTransform`, or the failpoints below
# would describe a plan shape that never re-merges.
explain_has_merge_sorting=$($CLICKHOUSE_CLIENT -q "
    SELECT count() > 0
    FROM (
        EXPLAIN PIPELINE
        $QUERY
        SETTINGS $SETTINGS, max_bytes_before_remerge_sort = $REMERGE_BYTES
    )
    WHERE explain ILIKE '%MergeSortingTransform%'
")
if [ "$explain_has_merge_sorting" != "1" ]; then
    echo "FAIL: the query does not sort through MergeSortingTransform"
fi

# The re-merge is a memory optimization only: the top of the order must not change.
expected=$($CLICKHOUSE_CLIENT -q "$QUERY SETTINGS $SETTINGS, max_bytes_before_remerge_sort = 0")
remerged=$($CLICKHOUSE_CLIENT -q "$QUERY SETTINGS $SETTINGS, max_bytes_before_remerge_sort = $REMERGE_BYTES")
expected_count=$(cut -f1 <<< "$expected")
if [ "$expected_count" != "500000" ]; then
    echo "FAIL: the query without re-merge returned $expected_count rows instead of 500000"
fi
if [ "$remerged" != "$expected" ]; then
    echo "FAIL: the re-merge changed the result: '$remerged' instead of '$expected'"
fi
echo "results-unchanged"

# Positive control and the killed arm for one poll site.
function check_failpoint()
{
    local fp="$1"
    local label="$2"
    local qid="${QID_PREFIX}_${label}"

    # Positive control: the un-killed query must pause again after `NOTIFY`, otherwise the
    # negative `WAIT` in the killed arm would pass even for a one-shot failpoint.
    local control_out="${CLICKHOUSE_TMP}/05829_control_${label}_${CLICKHOUSE_DATABASE}.out"
    rm -f "$control_out"

    $CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $fp"

    $CLICKHOUSE_CLIENT -q "$QUERY SETTINGS $SETTINGS, max_bytes_before_remerge_sort = $REMERGE_BYTES" > "$control_out" &
    local control_pid=$!

    wait_for_pause "$fp" "the un-killed control query did not pause at $fp"
    $CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT $fp"
    wait_for_pause "$fp" "the un-killed control query did not pause at the second $fp hit after NOTIFY"
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $fp"

    local control_status=0
    wait "$control_pid" || control_status=$?
    if [ "$control_status" != "0" ] || [ "$(cat "$control_out")" != "$expected" ]; then
        echo "FAIL: the un-killed control query did not complete after two pauses (status $control_status, result $(cat "$control_out"))"
    fi
    rm -f "$control_out"

    # The parked query must hold the accumulated blocks, otherwise the memory check below
    # would be measured on an empty query.
    $CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $fp"

    local client_err="${CLICKHOUSE_TMP}/05829_${label}_${CLICKHOUSE_DATABASE}.err"
    rm -f "$client_err"
    local baseline=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.metrics WHERE metric = 'MemoryTracking'")

    $CLICKHOUSE_CLIENT --query_id="$qid" -q "$QUERY SETTINGS $SETTINGS, max_bytes_before_remerge_sort = $REMERGE_BYTES" > /dev/null 2> "$client_err" &
    local client_pid=$!

    wait_for_pause "$fp" "the killed query never reached $fp"

    # The paused thread must belong to this test's query, or the kill below would hit
    # somebody else.
    local parked=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$qid'")
    if [ "$parked" != "1" ]; then
        echo "FAIL: the paused re-merge does not belong to the test query"
    fi

    local held=$($CLICKHOUSE_CLIENT -q "SELECT memory_usage FROM system.processes WHERE query_id = '$qid'")
    if [ "$held" -lt "$REMERGE_BYTES" ]; then
        echo "FAIL: the paused query holds only $held bytes, less than the re-merge threshold"
    fi

    $CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$qid' ASYNC FORMAT Null"

    # Wait until the kill is visible before releasing the thread, so the release cannot
    # race ahead of the kill flag being set.
    local cancelled=0
    for _ in {1..300}; do
        cancelled=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$qid' AND is_cancelled")
        [ "$cancelled" = "1" ] && break
        sleep 0.1
    done
    if [ "$cancelled" != "1" ]; then
        echo "FAIL: the killed query never became cancelled in system.processes"
    fi

    # Release the parked thread without disarming: the poll is right after the pause, so the
    # loop must throw instead of parking again at the next iteration.
    $CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT $fp"

    if timeout 5 $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $fp PAUSE"; then
        echo "FAIL: the killed re-merge paused again, so it kept merging instead of stopping at the cancellation poll"
    fi

    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $fp"

    local client_status=0
    wait "$client_pid" || client_status=$?

    if [ "$client_status" = "0" ]; then
        echo "FAIL: the killed query exited successfully"
    fi
    if ! grep -q -F "QUERY_WAS_CANCELLED" "$client_err"; then
        echo "FAIL: the killed query did not report QUERY_WAS_CANCELLED: $(cat "$client_err")"
    fi
    rm -f "$client_err"

    # The exception must free the accumulated blocks; `no-parallel` keeps other queries
    # from moving `MemoryTracking`.
    local released=0
    for _ in {1..300}; do
        local current=$($CLICKHOUSE_CLIENT -q "SELECT value FROM system.metrics WHERE metric = 'MemoryTracking'")
        if [ "$current" -lt $((baseline + held / 2)) ]; then
            released=1
            break
        fi
        sleep 0.1
    done
    if [ "$released" != "1" ]; then
        echo "FAIL: the killed query did not release the re-merged memory ($held bytes held, MemoryTracking $current instead of below $((baseline + held / 2)))"
    fi

    echo "$label"
}

check_failpoint "$FP_REMERGE" "cancelled-in-remerge-loop"
check_failpoint "$FP_MERGE" "cancelled-in-merge-batch"
