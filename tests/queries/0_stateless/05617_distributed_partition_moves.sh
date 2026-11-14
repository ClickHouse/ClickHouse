#!/usr/bin/env bash
# Tags: zookeeper, no-parallel
# no-parallel: uses the global shard_0 / shard_1 databases of test_cluster_two_shards_different_databases.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

MOVES_PARENT="/clickhouse/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}"
MOVES_PATH="${MOVES_PARENT}/partition_moves"
DIST_DEFINITION="(d Date, x UInt64) ENGINE = Distributed(test_cluster_two_shards_different_databases, '', data)"

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

function wait_for_exception()
{
    for _ in {1..600}; do
        has_exception=$($CLICKHOUSE_CLIENT -q "SELECT last_exception != '' FROM system.distributed_partition_moves WHERE database = currentDatabase() AND table = 'dist'")
        [ "$has_exception" = 1 ] && return
        sleep 0.1
    done
    echo "Timed out waiting for an error loading the partition moves"
}

function show_rows()
{
    $CLICKHOUSE_CLIENT -q "SELECT count(), sum(x) FROM dist"
    $CLICKHOUSE_CLIENT -q "SELECT _shard_num, toYYYYMM(d), count() FROM dist GROUP BY 1, 2 ORDER BY 1, 2"
}

$CLICKHOUSE_CLIENT -m -q "
    DROP DATABASE IF EXISTS shard_0;
    DROP DATABASE IF EXISTS shard_1;
    CREATE DATABASE shard_0;
    CREATE DATABASE shard_1;
    CREATE TABLE shard_0.data (d Date, x UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY x;
    CREATE TABLE shard_1.data (d Date, x UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY x;
    CREATE TABLE shard_0.copy (d Date, x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE shard_1.copy (d Date, x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO shard_0.data SELECT '2024-01-15', number FROM numbers(1, 3);
    INSERT INTO shard_1.data SELECT '2024-02-15', number FROM numbers(10, 3);
"

echo '-- the setting is experimental'
$CLICKHOUSE_CLIENT -q "CREATE TABLE dist $DIST_DEFINITION SETTINGS partition_moves_zookeeper_path = '$MOVES_PATH'" 2>&1 \
    | grep -o -m1 'SUPPORT_IS_DISABLED'
$CLICKHOUSE_CLIENT -q "CREATE TABLE remote_dist (d Date, x UInt64) ENGINE = Remote('127.0.0.1', 'shard_0', 'data') SETTINGS partition_moves_zookeeper_path = '${MOVES_PARENT}/remote_partition_moves'" 2>&1 \
    | grep -o -m1 'SUPPORT_IS_DISABLED'

echo '-- the node is created without moves'
$CLICKHOUSE_CLIENT --allow_experimental_distributed_partition_moves 1 -q "CREATE TABLE dist $DIST_DEFINITION SETTINGS partition_moves_zookeeper_path = '$MOVES_PATH'"
$CLICKHOUSE_CLIENT -q "SELECT value FROM system.zookeeper WHERE path = '$MOVES_PARENT' AND name = 'partition_moves' FORMAT TSVRaw"
show_rows

echo '-- the copy on the destination is skipped while queries read the source'
set_moves 'format version: 1\n202401 1 2 source\n'
wait_for_version 1
$CLICKHOUSE_CLIENT -q "ALTER TABLE shard_1.data ATTACH PARTITION ID '202401' FROM shard_0.data"
show_rows
$CLICKHOUSE_CLIENT -q "SELECT count() FROM dist SETTINGS optimize_trivial_count_query = 1"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM dist SETTINGS prefer_localhost_replica = 0"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM dist SETTINGS prefer_localhost_replica = 1"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM dist SETTINGS additional_table_filters = {'dist': 'x != 1'}"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM dist SETTINGS additional_table_filters = {'data': 'x != 2'}"
$CLICKHOUSE_CLIENT -q "SELECT version, moves, last_exception FROM system.distributed_partition_moves WHERE database = currentDatabase() AND table = 'dist'"

echo '-- INSERT SELECT does not copy the skipped copy'
$CLICKHOUSE_CLIENT -q "CREATE TABLE dist_copy (d Date, x UInt64) ENGINE = Distributed(test_cluster_two_shards_different_databases, '', copy, x)"
$CLICKHOUSE_CLIENT --parallel_distributed_insert_select 2 --distributed_foreground_insert 1 -q "INSERT INTO dist_copy SELECT * FROM dist"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(x) FROM dist_copy"

echo '-- the copy on the source is skipped while queries read the destination'
set_moves 'format version: 1\n202401 1 2 destination\n'
wait_for_version 2
show_rows

echo '-- the source copy is dropped and the move is removed'
$CLICKHOUSE_CLIENT -q "ALTER TABLE shard_0.data DROP PARTITION ID '202401'"
set_moves 'format version: 1\n'
wait_for_version 3
show_rows

echo '-- moves that cannot be parsed are not loaded'
set_moves 'format version: 1\n202401 1 1 source\n'
wait_for_exception
$CLICKHOUSE_CLIENT -q "SELECT version, moves FROM system.distributed_partition_moves WHERE database = currentDatabase() AND table = 'dist'"

echo '-- a move with a shard that the cluster does not have is rejected'
set_moves 'format version: 1\n202402 2 3 source\n'
wait_for_version 5
$CLICKHOUSE_CLIENT -q "SELECT count() FROM dist" 2>&1 \
    | grep -o -m1 'has a move with shard 3, but cluster test_cluster_two_shards_different_databases has 2 shards'

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE dist;
    DROP TABLE dist_copy;
    DROP DATABASE shard_0;
    DROP DATABASE shard_1;
"
