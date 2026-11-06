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

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_mutate_task_pause_in_prepare"
    rm -f "$KILL_OUT"
}
trap cleanup EXIT

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
$CLICKHOUSE_CLIENT -q "
    OPTIMIZE TABLE t FINAL SETTINGS max_execution_time = 2, optimize_throw_if_noop = 1
" 2>&1 | grep -o -m1 'Code: 159'

# 3. With timeout_overflow_mode = 'break' too.
$CLICKHOUSE_CLIENT -q "
    OPTIMIZE TABLE t PARTITION ID '0' FINAL
    SETTINGS max_execution_time = 2, timeout_overflow_mode = 'break', optimize_throw_if_noop = 1
" 2>&1 | grep -o -m1 'Timeout exceeded: elapsed'

# The queries above ended because they were cancelled, not because the mutation finished.
$CLICKHOUSE_CLIENT -q "SELECT is_done FROM system.mutations WHERE database = currentDatabase() AND table = 't'"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_mutate_task_pause_in_prepare"
wait_for_mutation t mutation_2.txt
$CLICKHOUSE_CLIENT -q "DROP TABLE t"
