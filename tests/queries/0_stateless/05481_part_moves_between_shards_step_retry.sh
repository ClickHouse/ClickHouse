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

# One client call for the whole scenario: the destination replica is taken away before the move
# exists (its Keeper tree stays, so the move is still accepted, but no replica of the destination
# will ever process a log entry of it), the move is queued, and the source table is restarted so
# its orchestrator steps at once instead of on its three second poll.
$CLICKHOUSE_CLIENT -q "
DETACH TABLE shard_dst;
ALTER TABLE shard_src MOVE PART '$PART' TO SHARD '/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/dst';
SELECT count() FROM system.part_moves_between_shards WHERE database = currentDatabase();
DETACH TABLE shard_src;
ATTACH TABLE shard_src;
"

# MOVE PART TO SHARD has no alter_sync, so the entry has to be polled; the round trip of one
# request paces the loop. Only the SYNC_DESTINATION try count proves that the inactive destination
# is what failed: step() resets num_tries to zero on every successful transition, so a count that
# an earlier state left behind is not evidence, while this one can never be reset or overtaken.
#
# A source step can fail for a retryable reason of its own, and the orchestrator then sleeps three
# seconds before retrying. Restarting the source table makes it retry at once. Such a restart is
# issued only on a source state that has already counted a try, which is exactly when the
# orchestrator is sleeping and no destination wait is in flight.
for _ in {1..400}; do
    entry=$($CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "SELECT state, num_tries > 0
        FROM system.part_moves_between_shards WHERE database = currentDatabase() AND table = 'shard_src'")
    case "$entry" in
        SYNC_DESTINATION*1) break ;;
        SYNC_SOURCE*1) $CLICKHOUSE_CLIENT -q "DETACH TABLE shard_src; ATTACH TABLE shard_src" ;;
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
