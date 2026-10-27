#!/usr/bin/env bash
# Tags: zookeeper, no-object-storage, no-random-merge-tree-settings, no-replicated-database, no-shared-merge-tree
# no-object-storage: object storage has no fsync, so FileSync stays 0 there.
# no-replicated-database, no-shared-merge-tree: the test controls fetches of each replica.

# With fsync_after_insert = 1 and fsync_after_insert_each_part = 0 an INSERT into ReplicatedMergeTree
# publishes its block in Keeper before it syncs its part. A retry of the same block on another replica
# is deduplicated against that part, which may not be durable anywhere yet. So the retry waits until it
# has a local copy of the part and syncs it, and fails if it cannot get one in time.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ZK_PATH="/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/t_fsync_dedup_other_replica"

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_fsync_dedup_other_replica_1 (k UInt64, s String)
    ENGINE = ReplicatedMergeTree('$ZK_PATH', 'r1') ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, fsync_after_insert = 1, fsync_after_insert_each_part = 0,
        sleep_before_commit_local_part_in_replicated_table_ms = 5000;
    CREATE TABLE t_fsync_dedup_other_replica_2 (k UInt64, s String)
    ENGINE = ReplicatedMergeTree('$ZK_PATH', 'r2') ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, fsync_after_insert = 1, fsync_after_insert_each_part = 0;
    SYSTEM STOP FETCHES t_fsync_dedup_other_replica_2;"

INSERT_SETTINGS="SETTINGS insert_deduplicate = 1, async_insert = 0, insert_deduplication_token = 'token'"
INSERT_DATA="SELECT number, toString(number) FROM numbers(100)"

# The original INSERT on r1 sleeps after its Keeper commit, before its part is even active, let alone synced.
$CLICKHOUSE_CLIENT --log_comment "fsync_dedup_other_replica_original" -q "INSERT INTO t_fsync_dedup_other_replica_1 $INSERT_SETTINGS $INSERT_DATA" &

# Wait until the block is published in Keeper.
for _ in {1..600}
do
    [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.zookeeper WHERE path = '$ZK_PATH/log'")" -gt 0 ] && break
    sleep 0.1
done

# The retry on r2 is deduplicated against the part of r1, but r2 cannot fetch it, so the retry is not acknowledged.
$CLICKHOUSE_CLIENT --log_comment "fsync_dedup_other_replica_retry_1" -q "
    INSERT INTO t_fsync_dedup_other_replica_2 $INSERT_SETTINGS, insert_quorum_timeout = 1000 $INSERT_DATA" 2>&1 | grep -o -m1 'TIMEOUT_EXCEEDED'

wait

# Once r2 can fetch the part, the retry gets a local copy and syncs it.
$CLICKHOUSE_CLIENT -q "SYSTEM START FETCHES t_fsync_dedup_other_replica_2"
$CLICKHOUSE_CLIENT --log_comment "fsync_dedup_other_replica_retry_2" -q "INSERT INTO t_fsync_dedup_other_replica_2 $INSERT_SETTINGS $INSERT_DATA"

$CLICKHOUSE_CLIENT -q "SELECT count(), sum(k) FROM t_fsync_dedup_other_replica_2"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
    SELECT log_comment, ProfileEvents['DuplicatedInsertedBlocks'] > 0, ProfileEvents['FileSync'] > 0
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'fsync_dedup_other_replica_retry_%'
    ORDER BY log_comment"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_fsync_dedup_other_replica_1; DROP TABLE t_fsync_dedup_other_replica_2"
