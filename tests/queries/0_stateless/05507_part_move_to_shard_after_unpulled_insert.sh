#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree, no-replicated-database
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# STOP PULLING does not wait for a pull in flight, so finish the post-CREATE one first.
$CLICKHOUSE_CLIENT -q "
CREATE TABLE src (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/src', 'r1') ORDER BY id
    SETTINGS part_moves_between_shards_enable = 1, part_moves_between_shards_delay_seconds = 0, assign_part_uuids = 1;
CREATE TABLE dst (id UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/dst', 'r1') ORDER BY id
    SETTINGS part_moves_between_shards_enable = 1, part_moves_between_shards_delay_seconds = 0, assign_part_uuids = 1;
SYSTEM SYNC REPLICA src PULL;
SYSTEM STOP PULLING REPLICATION LOG src;
"
$CLICKHOUSE_CLIENT -q "INSERT INTO src VALUES (1)"
PART=$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.parts WHERE database = currentDatabase() AND table = 'src' AND active")
# The insert's log entry is not in the queue yet: the MOVE has to pull it itself.
$CLICKHOUSE_CLIENT -q "
SYSTEM START PULLING REPLICATION LOG src;
ALTER TABLE src MOVE PART '$PART' TO SHARD '/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/dst';
SELECT count() FROM system.part_moves_between_shards WHERE database = currentDatabase() AND table = 'src';
DROP TABLE src SYNC;
DROP TABLE dst SYNC;
"
