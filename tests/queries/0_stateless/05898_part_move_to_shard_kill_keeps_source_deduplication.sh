#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree, no-replicated-database, no-fasttest
# Tag zookeeper: ReplicatedMergeTree and the part move entries live in Keeper.
# Tag no-shared-merge-tree: SharedMergeTree does not move parts between shards.
# Tag no-replicated-database: the test detaches and attaches replicated tables itself.
# Tag no-fasttest: the move advances on a three second background poll, so a run takes 5-10 s.

# KILL PART_MOVE_TO_SHARD of a move that has already attached the part on the destination drops the
# part there and keeps the deduplication hashes of the source table, so retried inserts into the
# source table are still deduplicated.

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
INSERT_SETTINGS="SETTINGS insert_deduplicate = 1, async_insert = 0"

$CLICKHOUSE_CLIENT -q "
CREATE TABLE src (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/src', 'r1') ORDER BY id
    SETTINGS part_moves_between_shards_enable = 1, part_moves_between_shards_delay_seconds = 0, assign_part_uuids = 1;
CREATE TABLE dst (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/dst', 'r1') ORDER BY id
    SETTINGS part_moves_between_shards_enable = 1, part_moves_between_shards_delay_seconds = 0, assign_part_uuids = 1;
SYSTEM STOP MERGES src;
INSERT INTO src $INSERT_SETTINGS VALUES (1);
INSERT INTO src $INSERT_SETTINGS VALUES (2);
INSERT INTO src $INSERT_SETTINGS VALUES (1);
SELECT count() FROM src WHERE id = 1;
"
PART=$($CLICKHOUSE_CLIENT -q "SELECT _part FROM src WHERE id = 2")

# The destination cannot fetch, so the move waits for the destination to clone the part.
$CLICKHOUSE_CLIENT -q "
SYSTEM STOP FETCHES dst;
ALTER TABLE src MOVE PART '$PART' TO SHARD '/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/dst';
"
wait_for "SELECT count() > 0 FROM system.replication_queue
    WHERE database = currentDatabase() AND table = 'dst' AND type = 'CLONE_PART_FROM_SHARD'" 1

# Disabling moves lets the waiting step finish but starts no other one.
$CLICKHOUSE_CLIENT -q "
ALTER TABLE src MODIFY SETTING part_moves_between_shards_enable = 0;
SYSTEM START FETCHES dst;
"
wait_for "$MOVE_STATE" DESTINATION_ATTACH
$CLICKHOUSE_CLIENT -q "$MOVE_STATE"

# A disabled table does not resume its moves by itself, so restart it after enabling them again.
# The destination cannot attach the part either, so the move waits for the attach.
$CLICKHOUSE_CLIENT -q "
SYSTEM STOP FETCHES dst;
ALTER TABLE src MODIFY SETTING part_moves_between_shards_enable = 1;
DETACH TABLE src;
ATTACH TABLE src;
"
wait_for "SELECT count() > 0 FROM system.replication_queue
    WHERE database = currentDatabase() AND table = 'dst' AND type = 'ATTACH_PART'" 1

# The move stops after the attach, in the last state that can still be killed.
$CLICKHOUSE_CLIENT -q "
ALTER TABLE src MODIFY SETTING part_moves_between_shards_enable = 0;
SYSTEM START FETCHES dst;
"
wait_for "$MOVE_STATE" SOURCE_DROP_PRE_DELAY
$CLICKHOUSE_CLIENT -q "
$MOVE_STATE;
SELECT count() FROM dst;
"

$CLICKHOUSE_CLIENT -q "KILL PART_MOVE_TO_SHARD WHERE database = currentDatabase() AND table = 'src'" > /dev/null
$CLICKHOUSE_CLIENT -q "
ALTER TABLE src MODIFY SETTING part_moves_between_shards_enable = 1;
DETACH TABLE src;
ATTACH TABLE src;
"
wait_for "$MOVE_STATE" CANCELLED
$CLICKHOUSE_CLIENT -q "
SELECT state, rollback FROM system.part_moves_between_shards WHERE database = currentDatabase() AND table = 'src';
SELECT count() FROM dst;
INSERT INTO src $INSERT_SETTINGS VALUES (1);
INSERT INTO src $INSERT_SETTINGS VALUES (2);
SELECT id, count() FROM src GROUP BY id ORDER BY id;
DROP TABLE src SYNC;
DROP TABLE dst SYNC;
"
