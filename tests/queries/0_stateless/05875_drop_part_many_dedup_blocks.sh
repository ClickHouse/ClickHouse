#!/usr/bin/env bash
# Tags: long, replica, no-parallel, no-fasttest, no-shared-merge-tree, no-replicated-database
# Tag no-parallel: uses a failpoint
# Tag no-replicated-database: the explicit `ReplicatedMergeTree` path gets `/auto_{shard}` appended on a replicated database
# Tag no-shared-merge-tree: depends on the ZooKeeper layout of ReplicatedMergeTree dedup blocks

# DROP PART removes the deduplication blocks of the part. A merged part can cover too many of them for the
# multi-request that commits the DROP_PART entry (it could exceed ZooKeeper's jute.maxbuffer), so they are removed
# in batches when the entry is executed. A DROP PART that is not committed keeps them.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ZK_PATH="/clickhouse/tables/$CLICKHOUSE_DATABASE/test_05875"

# A part with 120 deduplication blocks: 120 single-row inserts, merged.
create_table()
{
    $CLICKHOUSE_CLIENT --query "
        CREATE TABLE $1 (k UInt64) ENGINE = ReplicatedMergeTree('$ZK_PATH/$1', 'r1') ORDER BY k
        SETTINGS replicated_deduplication_window = 10000"
    insert_rows "$1"
    $CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE $1 FINAL"
}

insert_rows()
{
    $CLICKHOUSE_CLIENT --query "
        INSERT INTO $1 SELECT number FROM numbers(120) ORDER BY ALL
        SETTINGS min_insert_block_size_rows = 1, max_block_size = 1, insert_keeper_fault_injection_probability = 0"
}

active_part() { $CLICKHOUSE_CLIENT --query "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = '$1' AND active"; }
dedup_blocks() { $CLICKHOUSE_CLIENT --query "SELECT sum(numChildren) FROM system.zookeeper WHERE path = '$ZK_PATH/$1' AND name IN ('blocks', 'async_blocks', 'deduplication_hashes')"; }
row_count() { $CLICKHOUSE_CLIENT --query "SELECT count() FROM $1"; }

echo "-- DROP PART"
create_table t_drop
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_drop DROP PART '$(active_part t_drop)'"
row_count t_drop
dedup_blocks t_drop
insert_rows t_drop # not deduplicated
row_count t_drop

# Every sub-request of a multi shares its xid. Before the fix: one multi with 120 removals.
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS zookeeper_log"
$CLICKHOUSE_CLIENT --query "
    SELECT max(c) <= 100, count() > 1 FROM
    (
        SELECT count() AS c FROM system.zookeeper_log
        WHERE type = 'Request' AND op_num = 'Remove' AND startsWith(path, '$ZK_PATH/t_drop/')
          AND match(path, '/(blocks|async_blocks|deduplication_hashes)/')
        GROUP BY session_id, xid
    )"

echo "-- DROP PART that does not happen because a merge takes the part"
create_table t_merge
PART=$(active_part t_merge)
$CLICKHOUSE_CLIENT --query "INSERT INTO t_merge VALUES (1000)"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT rmt_drop_part_pause_before_commit"
$CLICKHOUSE_CLIENT --query "ALTER TABLE t_merge DROP PART '$PART'" 2>&1 | grep -o -m1 NO_SUCH_DATA_PART &
$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT rmt_drop_part_pause_before_commit PAUSE"
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE t_merge FINAL"
$CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT rmt_drop_part_pause_before_commit"
wait
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT rmt_drop_part_pause_before_commit"
dedup_blocks t_merge
insert_rows t_merge # deduplicated
row_count t_merge

$CLICKHOUSE_CLIENT --query "DROP TABLE t_drop"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_merge"
