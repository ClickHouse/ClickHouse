#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-replicated-database, no-shared-merge-tree
# no-parallel -- a server-wide failpoint pauses every plain `MergeTree` mutation task.
# no-fasttest -- a test that enables a failpoint runs alone, and such tests are kept out of the fast test.
# no-replicated-database -- the mutation task under pause is `MutatePlainMergeTreeTask`'s.
# no-shared-merge-tree -- the wait and the retry under test are `StorageMergeTree::selectPartsToMerge`'s.

# `OPTIMIZE FINAL` that cannot select because the parts are being mutated waits for the mutation and retries.
# The retry has to rebuild its snapshot of the allocated block numbers: a part inserted during the wait is
# above the snapshot taken before it, and a stale snapshot would leave that part out of the merge.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./mergetree_reservations.lib
. "$CUR_DIR"/mergetree_reservations.lib

MUTATE_FP="mt_mutate_task_pause_before_rename_part"
TABLE="t_refresh_after_wait"
OPTIMIZE_QUERY_ID="05494_optimize_${CLICKHOUSE_DATABASE}"

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $MUTATE_FP"
    $CLICKHOUSE_CLIENT --query "KILL QUERY WHERE query_id = '$OPTIMIZE_QUERY_ID' ASYNC" >/dev/null 2>&1
    wait
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $TABLE SYNC" 2>/dev/null
}
trap cleanup EXIT

# `max_bytes_to_merge_at_max_space_in_pool = 1` keeps background merges away from these parts; `OPTIMIZE` is not limited by it.
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS $TABLE SYNC;
    CREATE TABLE $TABLE (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1;
    INSERT INTO $TABLE VALUES (1, 0);
    INSERT INTO $TABLE VALUES (2, 0);
"

# The mutation rewrites both parts and parks each task right before it publishes the result.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $MUTATE_FP"
$CLICKHOUSE_CLIENT --query "ALTER TABLE $TABLE UPDATE v = v + 1 WHERE 1 SETTINGS mutations_sync = 0"
wait_failpoint $MUTATE_FP

# The parts are being mutated, so the selection fails and `OPTIMIZE FINAL` waits for them.
$CLICKHOUSE_CLIENT --query_id "$OPTIMIZE_QUERY_ID" \
    --query "OPTIMIZE TABLE $TABLE FINAL SETTINGS optimize_throw_if_noop = 1" &
optimize_pid=$!
while clickhouse_test_time_left; do
    $CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS text_log"
    waiting=$($CLICKHOUSE_CLIENT --query "
        SELECT count() > 0 FROM system.text_log
        WHERE query_id = '$OPTIMIZE_QUERY_ID' AND message LIKE 'Waiting for currently running merges%'")
    [[ "$waiting" == "1" ]] && break
    sleep 0.1
done
echo "OPTIMIZE FINAL is waiting: $waiting"

# Inserted while the `OPTIMIZE` waits: above the mutation's version and above the snapshot taken before the wait.
$CLICKHOUSE_CLIENT --query "INSERT INTO $TABLE VALUES (3, 0)"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $MUTATE_FP"
wait "$optimize_pid"
echo "OPTIMIZE FINAL exit code: $?"
wait_for_query_result "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = '$TABLE' AND NOT is_done" "0"

$CLICKHOUSE_CLIENT --query "
    SELECT 'active parts:', min_block_number, max_block_number, level, rows
    FROM system.parts
    WHERE database = currentDatabase() AND table = '$TABLE' AND active
    ORDER BY min_block_number
"
$CLICKHOUSE_CLIENT --query "SELECT id, v FROM $TABLE ORDER BY id"
