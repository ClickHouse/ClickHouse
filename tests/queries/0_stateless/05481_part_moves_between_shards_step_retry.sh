#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree, no-replicated-database
# Tag zookeeper: ReplicatedMergeTree and the part move entries live in Keeper.
# Tag no-shared-merge-tree: SharedMergeTree does not move parts between shards.
# Tag no-replicated-database: the test detaches and attaches replicated tables itself.

# A queued "MOVE PART ... TO SHARD" is driven by PartMovesBetweenShardsOrchestrator on the leader.
# The destination replica is detached before the move is queued, so no state transition of the
# entry can ever be acknowledged by the destination: the entry must stay queued and count a try,
# instead of the part leaving the source table. Detaching first leaves no window for a race.

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

# One client call for the whole scenario: the destination replica is taken away before the move
# exists (its Keeper tree stays, so the move is still accepted, but no replica of the destination
# will ever process a log entry of it), the move is queued, and the source table is restarted so
# its orchestrator runs at once instead of on its three second poll.
$CLICKHOUSE_CLIENT -q "
DETACH TABLE shard_dst;
ALTER TABLE shard_src MOVE PART '$PART' TO SHARD '/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/dst';
SELECT count() FROM system.part_moves_between_shards WHERE database = currentDatabase();
DETACH TABLE shard_src;
ATTACH TABLE shard_src;
"

# MOVE PART TO SHARD has no alter_sync, so the entry has to be polled; the round trip of one
# client paces the loop. The bound only limits a never satisfied wait: the asserts below then
# report the unfinished state instead of hanging.
for _ in {1..200}; do
    tries=$($CLICKHOUSE_CLIENT -q "SELECT max(num_tries) FROM system.part_moves_between_shards
        WHERE database = currentDatabase()")
    [ "${tries:-0}" != 0 ] && break
done

# The entry counted a try and is still queued, and the row never left the source table.
$CLICKHOUSE_CLIENT -q "
SELECT max(num_tries) > 0, count() FROM system.part_moves_between_shards WHERE database = currentDatabase();
SELECT count() FROM shard_src;
DROP TABLE shard_src SYNC;
ATTACH TABLE shard_dst;
DROP TABLE shard_dst SYNC;
"
