#!/usr/bin/env bash
# Tags: zookeeper, no-parallel
# no-parallel: uses the global shard_0 / shard_1 databases of test_cluster_two_shards_different_databases.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

MOVES_PARENT="/clickhouse/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}"
MOVES_PATH="${MOVES_PARENT}/partition_moves"

function set_moves()
{
    $CLICKHOUSE_CLIENT -q "INSERT INTO system.zookeeper (name, path, value) VALUES ('partition_moves', '$MOVES_PARENT', '$1')"
}

function wait_for_version()
{
    for _ in {1..600}; do
        version=$($CLICKHOUSE_CLIENT -q "SELECT version FROM system.distributed_partition_moves WHERE database = currentDatabase() AND table = 'dist'")
        [ "$version" = "$1" ] && return
        sleep 0.1
    done
    echo "Timed out waiting for version $1 of the partition moves, the loaded version is $version"
}

# The sharding key sends x = 2 to the first shard, and the partition p = 1 is moved to the second one.
function show_rows()
{
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM dist WHERE x = 2"
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM dist WHERE x = 2 SETTINGS optimize_skip_unused_shards = 1"
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM dist WHERE x IN (2, 3) SETTINGS optimize_skip_unused_shards = 1, optimize_skip_unused_shards_rewrite_in = 1"
    $CLICKHOUSE_CLIENT -q "SELECT x, count() FROM dist GROUP BY x SETTINGS optimize_skip_unused_shards = 1, optimize_distributed_group_by_sharding_key = 1"
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM dist WHERE x = 2 SETTINGS optimize_skip_unused_shards = 1, force_optimize_skip_unused_shards = 1" 2>&1 \
        | grep -o -m1 'UNABLE_TO_SKIP_UNUSED_SHARDS'
}

$CLICKHOUSE_CLIENT -m -q "
    DROP DATABASE IF EXISTS shard_0;
    DROP DATABASE IF EXISTS shard_1;
    CREATE DATABASE shard_0;
    CREATE DATABASE shard_1;
    CREATE TABLE shard_0.data (p UInt8, x UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY x;
    CREATE TABLE shard_1.data (p UInt8, x UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY x;
    INSERT INTO shard_0.data VALUES (1, 2), (2, 2);
"
$CLICKHOUSE_CLIENT --allow_experimental_distributed_partition_moves 1 -q "
    CREATE TABLE dist (p UInt8, x UInt64) ENGINE = Distributed(test_cluster_two_shards_different_databases, '', data, x)
    SETTINGS partition_moves_zookeeper_path = '$MOVES_PATH'"

echo '-- queries read the source'
set_moves 'format version: 1\n1 1 2 source\n'
wait_for_version 1
$CLICKHOUSE_CLIENT -q "ALTER TABLE shard_1.data ATTACH PARTITION ID '1' FROM shard_0.data"
show_rows

echo '-- queries read the destination'
set_moves 'format version: 1\n1 1 2 destination\n'
wait_for_version 2
show_rows

echo '-- the source copy is dropped and the move is removed'
$CLICKHOUSE_CLIENT -q "ALTER TABLE shard_0.data DROP PARTITION ID '1'"
set_moves 'format version: 1\n'
wait_for_version 3
show_rows
$CLICKHOUSE_CLIENT -q "SELECT _shard_num, p, x FROM dist ORDER BY 1, 2"

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE dist;
    DROP DATABASE shard_0;
    DROP DATABASE shard_1;
"
