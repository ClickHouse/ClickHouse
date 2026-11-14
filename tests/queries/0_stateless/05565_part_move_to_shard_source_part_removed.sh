#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree, no-replicated-database, no-fasttest
# Tag zookeeper: ReplicatedMergeTree and the part move entries live in Keeper.
# Tag no-shared-merge-tree: SharedMergeTree does not move parts between shards.
# Tag no-replicated-database: the test detaches and attaches replicated tables itself.
# Tag no-fasttest: the move advances on a three second background poll, so a run takes 7-14 s.

# A part removed from the source table (here by TRUNCATE) before "MOVE PART ... TO SHARD" attaches it
# on the destination makes the move fail with an error in system.part_moves_between_shards, and the
# move can then be cancelled with KILL PART_MOVE_TO_SHARD.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# One request per iteration paces the loop and an elapsed-time budget bounds it. The caller prints
# the final value, so a timeout shows up as a reference diff.
function wait_for()
{
    local query=$1 expected=$2
    local deadline=$((SECONDS + 60))
    while (( SECONDS < deadline )); do
        case "$($CLICKHOUSE_CURL -sS "$CLICKHOUSE_URL" -d "$query")" in
            "$expected"*) return ;;
        esac
    done
}

MOVE_STATE="SELECT state FROM system.part_moves_between_shards WHERE database = currentDatabase() AND table = 'src'"

$CLICKHOUSE_CLIENT -q "
CREATE TABLE src (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/src', 'r1') ORDER BY id
    SETTINGS part_moves_between_shards_enable = 1, part_moves_between_shards_delay_seconds = 0, assign_part_uuids = 1;
CREATE TABLE dst (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/dst', 'r1') ORDER BY id
    SETTINGS part_moves_between_shards_enable = 1, part_moves_between_shards_delay_seconds = 0, assign_part_uuids = 1;
INSERT INTO src VALUES (1);
"
PART=$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'src' AND active")

# The destination cannot fetch, so the move waits for the destination to clone the part.
$CLICKHOUSE_CLIENT -q "
SYSTEM STOP FETCHES dst;
ALTER TABLE src MOVE PART '$PART' TO SHARD '/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/dst';
"
wait_for "SELECT count() > 0 FROM system.replication_queue
    WHERE database = currentDatabase() AND table = 'dst' AND type = 'CLONE_PART_FROM_SHARD'" 1

# Disabling moves lets the waiting step finish but starts no other one, so the move stops right
# before it attaches the cloned part on the destination.
$CLICKHOUSE_CLIENT -q "
ALTER TABLE src MODIFY SETTING part_moves_between_shards_enable = 0;
SYSTEM START FETCHES dst;
"
wait_for "$MOVE_STATE" DESTINATION_ATTACH
$CLICKHOUSE_CLIENT -q "$MOVE_STATE"

$CLICKHOUSE_CLIENT --alter_sync 1 -q "TRUNCATE TABLE src"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'src' AND active"

# A disabled table does not resume its moves by itself, so restart it after enabling them again.
$CLICKHOUSE_CLIENT -q "
ALTER TABLE src MODIFY SETTING part_moves_between_shards_enable = 1;
DETACH TABLE src;
ATTACH TABLE src;
"
wait_for "SELECT num_tries > 0 FROM system.part_moves_between_shards WHERE database = currentDatabase() AND table = 'src'" 1
$CLICKHOUSE_CLIENT -q "SELECT state, num_tries > 0, last_exception LIKE '%NO_SUCH_DATA_PART%'
    FROM system.part_moves_between_shards WHERE database = currentDatabase() AND table = 'src'"

$CLICKHOUSE_CLIENT -q "KILL PART_MOVE_TO_SHARD WHERE database = currentDatabase() AND table = 'src'" > /dev/null
wait_for "$MOVE_STATE" CANCELLED
$CLICKHOUSE_CLIENT -q "
SELECT state, rollback FROM system.part_moves_between_shards WHERE database = currentDatabase() AND table = 'src';
SELECT count() FROM dst;
DROP TABLE src SYNC;
DROP TABLE dst SYNC;
"
