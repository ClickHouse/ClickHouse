#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-shared-merge-tree
# - no-parallel: the server-global failpoint `mt_mutate_task_pause_in_prepare` must keep this test's mutation paused.
# - no-fasttest: failpoints are not available in the fast test build.
# - no-shared-merge-tree: checks the `MergeTree` OPTIMIZE wait; SharedMergeTree has its own.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./mergetree_mutations.lib
. "$CUR_DIR"/mergetree_mutations.lib

KILL_ID="${CLICKHOUSE_TEST_UNIQUE_NAME}_kill"
KILL_OUT="${CLICKHOUSE_TMP}/${KILL_ID}.out"
TIMEOUT_ID="${CLICKHOUSE_TEST_UNIQUE_NAME}_timeout"
BREAK_ID="${CLICKHOUSE_TEST_UNIQUE_NAME}_break"
KILL_RELEASE_ID="${CLICKHOUSE_TEST_UNIQUE_NAME}_kill_release"
KILL_RELEASE_OUT="${CLICKHOUSE_TMP}/${KILL_RELEASE_ID}.out"
SUCCESS_ID="${CLICKHOUSE_TEST_UNIQUE_NAME}_success"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_mutate_task_pause_in_prepare"
    rm -f "$KILL_OUT" "$KILL_RELEASE_OUT"
}
trap cleanup EXIT

# Returns once the OPTIMIZE with query id $1 has logged that it waits for the running mutation, or its client $2 exited.
function wait_for_optimize_to_wait()
{
    local query_id="$1"
    local client_pid="$2"
    while clickhouse_test_time_left; do
        [[ $($CLICKHOUSE_CLIENT -q "
            SYSTEM FLUSH LOGS text_log;
            SELECT count() FROM system.text_log
            WHERE event_date >= yesterday() AND event_time >= now() - 600 AND query_id = '$query_id'
                AND message LIKE 'Waiting for currently running merges % to perform OPTIMIZE FINAL'
        ") -gt 0 ]] && return 0
        kill -0 "$client_pid" 2>/dev/null || return 0
        sleep 0.1
    done
    exit 1
}

# lock_acquire_timeout_for_background_operations bounds the wait for running merges, so a server that
# ignores the cancellation fails each OPTIMIZE below with CANNOT_ASSIGN_OPTIMIZE after 30 s instead of hanging.
# The part of partition 1 is inserted after the mutation, so the paused mutation of partition 0 is the only one.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t (k UInt64, v UInt64) ENGINE = MergeTree PARTITION BY k % 2 ORDER BY k
    SETTINGS lock_acquire_timeout_for_background_operations = 30;
    INSERT INTO t VALUES (0, 0);
    SYSTEM ENABLE FAILPOINT mt_mutate_task_pause_in_prepare;
    ALTER TABLE t UPDATE v = v + 1 WHERE 1;
    SYSTEM WAIT FAILPOINT mt_mutate_task_pause_in_prepare PAUSE;
    INSERT INTO t VALUES (1, 1);
"

# 1. KILL QUERY stops OPTIMIZE FINAL of a partition waiting for the running mutation of its part.
$CLICKHOUSE_CLIENT --query_id "$KILL_ID" -q "
    OPTIMIZE TABLE t PARTITION ID '0' FINAL SETTINGS optimize_throw_if_noop = 1
" > "$KILL_OUT" 2>&1 &
wait_for_query_to_start "$KILL_ID" 120 $!
$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$KILL_ID' ASYNC FORMAT Null"
wait
grep -o -m1 'Code: 394' "$KILL_OUT"

# 2. max_execution_time stops the same wait in the parallel OPTIMIZE FINAL of the whole table.
$CLICKHOUSE_CLIENT --query_id "$TIMEOUT_ID" -q "
    OPTIMIZE TABLE t FINAL SETTINGS max_execution_time = 2, optimize_throw_if_noop = 1
" 2>&1 | grep -o -m1 'Code: 159'

# 3. With timeout_overflow_mode = 'break' too.
$CLICKHOUSE_CLIENT --query_id "$BREAK_ID" -q "
    OPTIMIZE TABLE t PARTITION ID '0' FINAL
    SETTINGS max_execution_time = 2, timeout_overflow_mode = 'break', optimize_throw_if_noop = 1
" 2>&1 | grep -o -m1 'Timeout exceeded: elapsed'

# Both ended at their max_execution_time, well before the 30 s lock_acquire_timeout_for_background_operations.
$CLICKHOUSE_CLIENT -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT count() FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase()
        AND query_id IN ('$TIMEOUT_ID', '$BREAK_ID') AND type = 'ExceptionBeforeStart' AND query_duration_ms < 20000
"

# The queries above ended because they were cancelled, not because the mutation finished.
$CLICKHOUSE_CLIENT -q "SELECT is_done FROM system.mutations WHERE database = currentDatabase() AND table = 't'"

# 4. A KILL sent together with the release of the mutation still stops the OPTIMIZE: its wait can end on the
#    finished mutation, and it must not go on to merge the partition, whose part therefore keeps level 0.
$CLICKHOUSE_CLIENT --query_id "$KILL_RELEASE_ID" -q "
    OPTIMIZE TABLE t PARTITION ID '0' FINAL SETTINGS optimize_throw_if_noop = 1
" > "$KILL_RELEASE_OUT" 2>&1 &
wait_for_optimize_to_wait "$KILL_RELEASE_ID" $!
$CLICKHOUSE_CLIENT -q "
    KILL QUERY WHERE query_id = '$KILL_RELEASE_ID' ASYNC FORMAT Null;
    SYSTEM DISABLE FAILPOINT mt_mutate_task_pause_in_prepare;
"
wait
grep -o -m1 'Code: 394' "$KILL_RELEASE_OUT"
wait_for_mutation t mutation_2.txt
$CLICKHOUSE_CLIENT -q "SELECT level FROM system.parts WHERE database = currentDatabase() AND table = 't' AND partition_id = '0' AND active"
$CLICKHOUSE_CLIENT -q "DROP TABLE t"

# 5. Without a cancellation, a waiting OPTIMIZE FINAL merges as soon as the mutation it waits for finishes.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t2 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS lock_acquire_timeout_for_background_operations = 30;
    INSERT INTO t2 VALUES (0, 0);
    SYSTEM ENABLE FAILPOINT mt_mutate_task_pause_in_prepare;
    ALTER TABLE t2 UPDATE v = v + 1 WHERE 1;
    SYSTEM WAIT FAILPOINT mt_mutate_task_pause_in_prepare PAUSE;
"
$CLICKHOUSE_CLIENT --query_id "$SUCCESS_ID" -q "OPTIMIZE TABLE t2 FINAL SETTINGS optimize_throw_if_noop = 1" &
OPTIMIZE_PID=$!
wait_for_optimize_to_wait "$SUCCESS_ID" $OPTIMIZE_PID
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_mutate_task_pause_in_prepare"
wait $OPTIMIZE_PID && echo "OPTIMIZE merged after the mutation finished"
$CLICKHOUSE_CLIENT -q "SELECT level FROM system.parts WHERE database = currentDatabase() AND table = 't2' AND active"
$CLICKHOUSE_CLIENT -q "DROP TABLE t2"
