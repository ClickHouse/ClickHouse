#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database, no-ordinary-database
# - no-fasttest: relies on failpoints, which are not available in the fast test build.
# - no-parallel: the test toggles the server-global failpoint `merge_task_projection_stage_pause`,
#   which would pause projection merges of other tests running at the same time.
# - no-replicated-database, no-ordinary-database: transactions are not supported there.

# OPTIMIZE ... FINAL that waits for a merge done in a transaction of another session must not take the part
# made by that merge for the merged partition and report success: the transaction can still be rolled back.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./transactions.lib
. "$CUR_DIR"/transactions.lib

table=t_optimize_final_after_wait_transaction

cleanup() {
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT merge_task_projection_stage_pause" 2>/dev/null
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $table SYNC" 2>/dev/null
}
trap cleanup EXIT

# A projection makes every merge go through the projection-merge stage, where the failpoint pauses it.
# `max_bytes_to_merge_at_max_space_in_pool = 1` keeps background merges away from these parts.
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE $table (k UInt64, v UInt64, PROJECTION by_v (SELECT k, v ORDER BY v))
    ENGINE = MergeTree ORDER BY k
    SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1"

for i in 1 2 3; do
    $CLICKHOUSE_CLIENT --query "INSERT INTO $table SELECT number, $i FROM numbers(1000)"
done

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT merge_task_projection_stage_pause"

tx 1 "BEGIN TRANSACTION"
tx 1 "OPTIMIZE TABLE $table FINAL" &
first_pid=$!
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT merge_task_projection_stage_pause PAUSE"

# The second OPTIMIZE, outside of the transaction, waits for the merge of the first one.
second_query_id="${CLICKHOUSE_DATABASE}_second"
$CLICKHOUSE_CLIENT --query_id "$second_query_id" --query "OPTIMIZE TABLE $table FINAL" >/dev/null 2>&1 &
second_pid=$!

waiting=no
for _ in {1..120}; do
    if [[ $($CLICKHOUSE_CLIENT --query "
        SYSTEM FLUSH LOGS text_log;
        SELECT count() FROM system.text_log
        WHERE event_date >= yesterday() AND query_id = '$second_query_id'
            AND message LIKE 'Waiting for currently running merges%'") -ge 1 ]]; then
        waiting=yes
        break
    fi
    sleep 0.5
done
echo "second OPTIMIZE waits for the running merge: $waiting"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT merge_task_projection_stage_pause"
wait "$first_pid"

# The part made by the merge of the transaction is not committed, so the second OPTIMIZE does not skip it,
# and merging it outside of the transaction fails.
if wait "$second_pid"; then
    echo "second OPTIMIZE succeeded: yes"
else
    echo "second OPTIMIZE succeeded: no"
fi

tx 1 "ROLLBACK"
$CLICKHOUSE_CLIENT --query "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = '$table' AND active"
