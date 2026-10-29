#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-shared-merge-tree, no-replicated-database, no-fasttest
# no-parallel: the failpoints are server-global; one of them pauses every part check on the server.
# no-shared-merge-tree: the check and its stop flag live in `ReplicatedMergeTreePartCheckThread`.
# no-replicated-database: the failpoints and the restart are issued to one server, so the table must have one replica.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A part check reads every file of the part to the end, and used to look at the stop flag of
# `ReplicatedMergeTreePartCheckThread` only between projections and at the very end. On object storage a
# single column file can be tens of gigabytes, so `partialShutdown` -> `part_check_thread.stop()` blocked
# on the running check for the whole read, and a replica whose Keeper session had expired stayed read-only
# until the check was done. The read loop now polls the stop flag between blocks.
#
# `CHECK TABLE` on a `ReplicatedMergeTree` table runs the same check with the same stop flag, so it plays
# the in-flight check here: it is held after the first block of a file, a replica restart raises the stop
# flag, and the check is released for exactly one more block. A check that polls the flag gives up there.
# A check that reads on pauses again at the next block and never finishes.

table="t_05458"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS $table SYNC;
    CREATE TABLE $table (a UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/$table', 'r1')
    ORDER BY a
    SETTINGS min_bytes_for_wide_part = 0;
    -- Several compressed blocks per column file, so that the check has blocks left after the one it is held on.
    INSERT INTO $table SELECT number FROM numbers(2000000);
"

$CLICKHOUSE_CLIENT --query "
    SYSTEM ENABLE FAILPOINT check_data_part_pause_after_block_read;
    SYSTEM ENABLE FAILPOINT rmt_part_check_thread_pause_in_stop;
"

# The failpoints are server-global, so they must be disabled even if the script aborts before the explicit
# disables below. Otherwise every part check and every replica shutdown on the server stays paused.
trap '$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT check_data_part_pause_after_block_read; SYSTEM DISABLE FAILPOINT rmt_part_check_thread_pause_in_stop" > /dev/null 2>&1 || true' EXIT

check_out="${CLICKHOUSE_TMP}/05458_check_${CLICKHOUSE_DATABASE}.out"
rm -f "$check_out"
$CLICKHOUSE_CLIENT --check_query_single_value_result 1 --query "CHECK TABLE $table" > "$check_out" 2>&1 &
check_pid=$!

# The check is now held inside the first column file, after its first block.
if ! timeout 120 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT check_data_part_pause_after_block_read PAUSE" > /dev/null 2>&1; then
    echo "FAIL: the check did not pause inside a file"
fi

# The restart raises the stop flag and is then held, before it waits for the check thread.
restart_out="${CLICKHOUSE_TMP}/05458_restart_${CLICKHOUSE_DATABASE}.out"
rm -f "$restart_out"
$CLICKHOUSE_CLIENT --query "SYSTEM RESTART REPLICA $table" > "$restart_out" 2>&1 &
restart_pid=$!

if ! timeout 120 $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT rmt_part_check_thread_pause_in_stop PAUSE" > /dev/null 2>&1; then
    echo "FAIL: the restart did not raise the stop flag"
fi

# Release the check for one more block. The stop flag is already raised, so the poll between blocks ends
# the read, and the check reports that it was cancelled. Bounded, so that a check which reads on and pauses
# again is reported instead of blocking the test until its timeout.
$CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT check_data_part_pause_after_block_read"

check_finished=0
for _ in {1..600}; do
    if ! kill -0 "$check_pid" 2>/dev/null; then
        check_finished=1
        break
    fi
    sleep 0.1
done
if [[ "$check_finished" != "1" ]]; then
    echo "FAIL: the check did not stop after the stop flag was raised"
fi

# Lets a check that is still paused finish, so that the test does not hang on `wait` after a failure.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT check_data_part_pause_after_block_read"
wait "$check_pid" 2>/dev/null || true

if grep -q "ABORTED" "$check_out"; then
    echo "check cancelled by the shutdown"
else
    echo "FAIL: unexpected check result: $(cat "$check_out")"
fi
rm -f "$check_out"

# Let the restart finish: the check thread has nothing running anymore, so the stop returns at once.
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT rmt_part_check_thread_pause_in_stop"
wait "$restart_pid" || echo "FAIL: restart failed: $(cat "$restart_out")"
rm -f "$restart_out"

# The restarted replica checks its part to the end.
$CLICKHOUSE_CLIENT --check_query_single_value_result 1 --query "CHECK TABLE $table"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM $table"

$CLICKHOUSE_CLIENT --query "DROP TABLE $table SYNC"
