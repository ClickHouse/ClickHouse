#!/usr/bin/env bash
# Tags: zookeeper, no-object-storage, no-random-merge-tree-settings, no-replicated-database, no-shared-merge-tree
# no-object-storage: object storage has no fsync, so FileSync stays 0 there.
# no-replicated-database, no-shared-merge-tree: the test relies on a single local replica.

# With fsync_after_insert = 1 and fsync_after_insert_each_part = 0 an INSERT into ReplicatedMergeTree
# publishes its block in Keeper before it makes the local part active. A retry of the same block that
# runs in between is deduplicated against the still PreActive part, and it has to sync that part
# before it is acknowledged.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ZK_PATH="/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_fsync_dedup_pre_active"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_fsync_dedup_pre_active (k UInt64, s String)
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_fsync_dedup_pre_active', 'r1') ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, fsync_after_insert = 1, fsync_after_insert_each_part = 0,
        sleep_before_commit_local_part_in_replicated_table_ms = 5000"

INSERT_QUERY="INSERT INTO t_fsync_dedup_pre_active SETTINGS insert_deduplicate = 1, async_insert = 0, insert_deduplication_token = 'token'
    SELECT number, toString(number) FROM numbers(100)"

# The original INSERT sleeps after its Keeper commit, before it makes the local part active.
$CLICKHOUSE_CLIENT --log_comment "fsync_dedup_pre_active_original" -q "$INSERT_QUERY" &

# Wait until the block is published in Keeper.
for _ in {1..600}
do
    [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.zookeeper WHERE path = '$ZK_PATH/log'")" -gt 0 ] && break
    sleep 0.1
done

# The retry is deduplicated against the part that is not active yet.
$CLICKHOUSE_CLIENT --log_comment "fsync_dedup_pre_active_retry" -q "$INSERT_QUERY"

wait

$CLICKHOUSE_CLIENT -q "SELECT count(), sum(k) FROM t_fsync_dedup_pre_active"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
    SELECT log_comment, ProfileEvents['DuplicatedInsertedBlocks'] > 0, ProfileEvents['FileSync'] > 0
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'fsync_dedup_pre_active_%'
    ORDER BY log_comment"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_fsync_dedup_pre_active"
