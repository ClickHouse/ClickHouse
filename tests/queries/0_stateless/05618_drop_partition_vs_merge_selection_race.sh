#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database, no-shared-merge-tree
# no-fasttest, no-parallel: failpoints are server-global state.
# no-replicated-database, no-shared-merge-tree: the failpoints are in plain `MergeTree`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_drop_partition_pause_after_merges_stopped" 2>/dev/null
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_merge_selecting_task_pause_after_selection" 2>/dev/null
}
trap cleanup EXIT

# `DROP PARTITION` stops the merges of the partition and then removes its parts. A merge selected in between
# must not take the parts of that partition: it would run after the drop, over removed parts, and its result
# could bring the dropped rows back.

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (p UInt8, x UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY tuple();
    SYSTEM STOP MERGES t;
    INSERT INTO t SELECT 1, number FROM numbers(10) SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1;
    SYSTEM ENABLE FAILPOINT mt_drop_partition_pause_after_merges_stopped;
"

$CLICKHOUSE_CLIENT -q "ALTER TABLE t DROP PARTITION 1" &
drop_pid=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT mt_drop_partition_pause_after_merges_stopped PAUSE"

# The drop has blocked the merges of partition 1 and has not removed its parts yet. Let the merge selection
# of `t` run once and hold it right after it has released the selection mutex (the failpoint only holds the
# selection of a table with a blocked partition, so other tables do not take its place).
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT mt_merge_selecting_task_pause_after_selection"
$CLICKHOUSE_CLIENT -q "SYSTEM START MERGES t"
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT mt_merge_selecting_task_pause_after_selection PAUSE"

# Let the drop finish, then let the selection go on: a merge selected in partition 1 would run now.
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_drop_partition_pause_after_merges_stopped"
wait $drop_pid
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT mt_merge_selecting_task_pause_after_selection"
sleep 1

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS part_log"
$CLICKHOUSE_CLIENT -q "SELECT 'rows after drop', count() FROM t"
$CLICKHOUSE_CLIENT -q "
    SELECT 'merges of partition 1', count()
    FROM system.part_log
    WHERE database = currentDatabase() AND table = 't' AND event_type = 'MergeParts' AND partition_id = '1'
"
$CLICKHOUSE_CLIENT -q "DROP TABLE t"
