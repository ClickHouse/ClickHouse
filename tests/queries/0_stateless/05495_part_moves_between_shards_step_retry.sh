#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree, no-replicated-database
# Tag zookeeper: ReplicatedMergeTree and the part move entries live in Keeper.
# Tag no-shared-merge-tree: SharedMergeTree does not move parts between shards.
# Tag no-replicated-database: the test detaches and attaches replicated tables itself.

# A queued "MOVE PART ... TO SHARD" is driven by PartMovesBetweenShardsOrchestrator on the leader.
# The destination replica is detached before the move is queued, so the entry walks TODO ->
# SYNC_SOURCE -> SYNC_DESTINATION and then stays in SYNC_DESTINATION: that state waits for the
# replicas of the destination shard to process its log entry, and the detached replica has no
# is_active node, so every try throws TIMEOUT_EXCEEDED and num_tries only grows. SYNC_DESTINATION
# with a positive num_tries is therefore stable until cleanup, and the part can never leave the
# source table because SOURCE_DROP is behind SYNC_DESTINATION.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS shard_src SYNC;
DROP TABLE IF EXISTS shard_dst SYNC;
CREATE TABLE shard_src (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/src', 'r1') ORDER BY id
    SETTINGS part_moves_between_shards_enable = 1, part_moves_between_shards_delay_seconds = 0, assign_part_uuids = 1;
CREATE TABLE shard_dst (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/dst', 'r1') ORDER BY id
    SETTINGS part_moves_between_shards_enable = 1, part_moves_between_shards_delay_seconds = 0, assign_part_uuids = 1;
INSERT INTO shard_src VALUES (1);
"

PART=$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.parts
    WHERE database = currentDatabase() AND table = 'shard_src' AND active")

# One client call for the whole scenario: queue execution on the source is blocked first, the
# destination replica is taken away before the move exists (its Keeper tree stays, so the move is
# still accepted, but no replica of the destination will ever process a log entry of it), the move
# is queued, and the source table is restarted so its orchestrator steps at once instead of on its
# three second poll. The block has to be issued again after the restart: an action lock is keyed by
# the storage object (ActionLocksManager::storage_locks) and ATTACH makes a new one.
$CLICKHOUSE_CLIENT -q "
SYSTEM STOP REPLICATION QUEUES shard_src;
DETACH TABLE shard_dst;
ALTER TABLE shard_src MOVE PART '$PART' TO SHARD '/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/dst';
SELECT count() FROM system.part_moves_between_shards WHERE database = currentDatabase();
DETACH TABLE shard_src;
ATTACH TABLE shard_src;
SYSTEM STOP REPLICATION QUEUES shard_src;
"

# SYNC_SOURCE pushes a SYNC_PINNED_PART_UUIDS log entry and waits for the source replica itself to
# process it. That wait lists the replica queue and only then watches the node it found, so an
# entry that is executed in between makes the source step fail and costs a three second retry.
# Blocking queue execution keeps the node in place while the wait looks for it; pulling the log is
# a different lock, so the entry still reaches the queue. Wait for it there, then let it run.
deadline=$((SECONDS + 30))
while (( SECONDS < deadline )); do
    queued=$($CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "SELECT
        (SELECT count() FROM system.replication_queue WHERE database = currentDatabase()
            AND table = 'shard_src' AND type = 'SYNC_PINNED_PART_UUIDS') > 0
        OR (SELECT count() FROM system.part_moves_between_shards WHERE database = currentDatabase()
            AND table = 'shard_src' AND state = 'SYNC_DESTINATION') > 0")
    case "$queued" in
        1*) break ;;
    esac
done

$CLICKHOUSE_CLIENT -q "SYSTEM START REPLICATION QUEUES shard_src"

# MOVE PART TO SHARD has no alter_sync, so the entry has to be polled; the round trip of one
# request paces the loop and an elapsed-time budget bounds it. Only the SYNC_DESTINATION try count
# proves that the inactive destination is what failed: step() resets num_tries to zero on every
# successful transition, so a count that an earlier state left behind is not evidence, while this
# one can never be reset or overtaken. The table stays attached for the whole wait, so the
# background transition that is being waited for is never interrupted by the test itself.
deadline=$((SECONDS + 60))
while (( SECONDS < deadline )); do
    entry=$($CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "SELECT state, num_tries > 0
        FROM system.part_moves_between_shards WHERE database = currentDatabase() AND table = 'shard_src'")
    case "$entry" in
        SYNC_DESTINATION*1) break ;;
    esac
done

# The entry is stuck in destination synchronization with a counted try, and the row never left the
# source table.
$CLICKHOUSE_CLIENT -q "
SELECT state, num_tries > 0 FROM system.part_moves_between_shards
    WHERE database = currentDatabase() AND table = 'shard_src';
SELECT count() FROM shard_src;
DROP TABLE shard_src SYNC;
ATTACH TABLE shard_dst;
DROP TABLE shard_dst SYNC;
"
