#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database, no-ordinary-database, no-shared-merge-tree
# no-fasttest, no-parallel: a failpoint is server-global state, and transactions need the test configuration.
# no-replicated-database, no-shared-merge-tree: the failpoint is in plain `MergeTree`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_merge_task_pause_before_transaction_commit" 2>/dev/null
}
trap cleanup EXIT

# A background merge of a table with transactions publishes its result part and outdates the source parts
# before it commits its transaction. `DROP PARTITION` of that partition must wait for the merge to finish:
# the result part cannot be removed non-transactionally while its creation is not committed.

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (p UInt8, x UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY tuple();
    SYSTEM STOP MERGES t;
"
# An insert in a transaction enables transactions on the table, so background merges run in their own transaction.
# async_insert is disabled because it is incompatible with implicit_transaction.
$CLICKHOUSE_CLIENT --implicit_transaction 1 --async_insert 0 -q "INSERT INTO t VALUES (1, 0)"
$CLICKHOUSE_CLIENT -q "INSERT INTO t SELECT 1, number FROM numbers(10) SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1"

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT mt_merge_task_pause_before_transaction_commit"
$CLICKHOUSE_CLIENT -q "SYSTEM START MERGES t"
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT mt_merge_task_pause_before_transaction_commit PAUSE"

drop_query_id="${CLICKHOUSE_DATABASE}_drop_$RANDOM"
$CLICKHOUSE_CLIENT --query_id "$drop_query_id" -q "ALTER TABLE t DROP PARTITION 1" &
drop_pid=$!

# Without the fix the drop fails at once. With the fix it waits for the merge.
while [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$drop_query_id'") == 0 ]] && kill -0 $drop_pid 2>/dev/null
do
    sleep 0.1
done
sleep 1
$CLICKHOUSE_CLIENT -q "SELECT 'drop is waiting', count() FROM system.processes WHERE query_id = '$drop_query_id'"

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_merge_task_pause_before_transaction_commit"
wait $drop_pid
echo "drop exit code $?"

$CLICKHOUSE_CLIENT -q "SELECT 'rows after drop', count() FROM t"
$CLICKHOUSE_CLIENT -q "DROP TABLE t"
