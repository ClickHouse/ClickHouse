#!/usr/bin/env bash
# Tags: zookeeper, no-parallel
# no-parallel: uses the global shard_0 / shard_1 databases of test_cluster_two_shards_different_databases.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

MAP_PARENT="/clickhouse/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}"
MAP_PATH="${MAP_PARENT}/bucket_map"
DIST_DEFINITION="(key UInt64) ENGINE = Distributed(test_cluster_two_shards_different_databases, '', data, key)"

function set_map()
{
    $CLICKHOUSE_CLIENT -q "INSERT INTO system.zookeeper (name, path, value) VALUES ('bucket_map', '$MAP_PARENT', '$1')"
}

function wait_for_map_version()
{
    for _ in {1..600}; do
        version=$($CLICKHOUSE_CLIENT -q "SELECT version FROM system.distributed_bucket_maps WHERE database = currentDatabase() AND table = 'dist'")
        [ "$version" = "$1" ] && return
        sleep 0.1
    done
    echo "Timed out waiting for version $1 of the bucket map, the loaded version is $version"
}

function wait_for_map_exception()
{
    for _ in {1..600}; do
        has_exception=$($CLICKHOUSE_CLIENT -q "SELECT last_exception != '' FROM system.distributed_bucket_maps WHERE database = currentDatabase() AND table = 'dist'")
        [ "$has_exception" = 1 ] && return
        sleep 0.1
    done
    echo "Timed out waiting for an error loading the bucket map"
}

function shard_keys()
{
    $CLICKHOUSE_CLIENT -q "SELECT '$1', arraySort(groupArray(key)) FROM $1.data"
}

$CLICKHOUSE_CLIENT -m -q "
    DROP DATABASE IF EXISTS shard_0;
    DROP DATABASE IF EXISTS shard_1;
    CREATE DATABASE shard_0;
    CREATE DATABASE shard_1;
    CREATE TABLE shard_0.data (key UInt64) ENGINE = MergeTree ORDER BY key;
    CREATE TABLE shard_1.data (key UInt64) ENGINE = MergeTree ORDER BY key;
"

echo '-- the setting is experimental'
$CLICKHOUSE_CLIENT -q "CREATE TABLE dist $DIST_DEFINITION SETTINGS bucket_map_zookeeper_path = '$MAP_PATH', bucket_map_buckets = 4" 2>&1 \
    | grep -o -m1 'SUPPORT_IS_DISABLED'
$CLICKHOUSE_CLIENT -q "CREATE TABLE remote_dist (key UInt64) ENGINE = Remote('127.0.0.1', 'shard_0', 'data', key) SETTINGS bucket_map_zookeeper_path = '${MAP_PARENT}/remote_bucket_map', bucket_map_buckets = 4" 2>&1 \
    | grep -o -m1 'SUPPORT_IS_DISABLED'

echo '-- a map that does not exist needs the number of buckets'
$CLICKHOUSE_CLIENT --allow_experimental_distributed_bucket_map 1 -q "CREATE TABLE dist $DIST_DEFINITION SETTINGS bucket_map_zookeeper_path = '$MAP_PATH'" 2>&1 \
    | grep -o -m1 'Set `bucket_map_buckets` to create it'

echo '-- a new map splits the buckets evenly between the shards'
$CLICKHOUSE_CLIENT --allow_experimental_distributed_bucket_map 1 -q "CREATE TABLE dist $DIST_DEFINITION SETTINGS bucket_map_zookeeper_path = '$MAP_PATH', bucket_map_buckets = 4"
$CLICKHOUSE_CLIENT -q "SELECT value FROM system.zookeeper WHERE path = '$MAP_PARENT' AND name = 'bucket_map' FORMAT TSVRaw"

echo '-- a row goes to the shard of the bucket sharding_key % buckets'
$CLICKHOUSE_CLIENT --distributed_foreground_insert 1 -q "INSERT INTO dist SELECT number FROM numbers(8)"
shard_keys shard_0
shard_keys shard_1
$CLICKHOUSE_CLIENT -q "SELECT version, buckets, ranges, last_exception FROM system.distributed_bucket_maps WHERE database = currentDatabase() AND table = 'dist'"

echo '-- shards are skipped by the bucket map'
$CLICKHOUSE_CLIENT -q "SELECT _shard_num, key FROM dist WHERE key = 5 SETTINGS optimize_skip_unused_shards = 1, force_optimize_skip_unused_shards = 1"
$CLICKHOUSE_CLIENT -q "SELECT _shard_num, key FROM dist WHERE key IN (1, 2, 5) ORDER BY key SETTINGS optimize_skip_unused_shards = 1, optimize_skip_unused_shards_rewrite_in = 1"

echo '-- a changed map is loaded and routes the new rows'
set_map 'format version: 1\nbuckets: 4\n0 0 1\n1 3 2\n'
wait_for_map_version 1
$CLICKHOUSE_CLIENT --distributed_foreground_insert 1 -q "INSERT INTO dist SELECT number FROM numbers(8, 8)"
shard_keys shard_0
shard_keys shard_1
$CLICKHOUSE_CLIENT -q "SELECT _shard_num, key FROM dist WHERE key = 13 SETTINGS optimize_skip_unused_shards = 1, force_optimize_skip_unused_shards = 1"

echo '-- a map that cannot be parsed is not loaded'
set_map 'format version: 1\nbuckets: 4\n0 2 1\n'
wait_for_map_exception
$CLICKHOUSE_CLIENT -q "SELECT version, ranges FROM system.distributed_bucket_maps WHERE database = currentDatabase() AND table = 'dist'"

echo '-- a map with a shard that the cluster does not have is rejected'
set_map 'format version: 1\nbuckets: 4\n0 1 1\n2 3 3\n'
wait_for_map_version 3
$CLICKHOUSE_CLIENT --distributed_foreground_insert 1 -q "INSERT INTO dist VALUES (100)" 2>&1 \
    | grep -o -m1 'assigns buckets to shard 3, but cluster test_cluster_two_shards_different_databases has 2 shards'

echo '-- the table is attached without the experimental setting'
set_map 'format version: 1\nbuckets: 4\n0 1 1\n2 3 2\n'
$CLICKHOUSE_CLIENT -q "DETACH TABLE dist"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE dist"
$CLICKHOUSE_CLIENT --distributed_foreground_insert 1 -q "INSERT INTO dist VALUES (102)"
shard_keys shard_1

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE dist;
    DROP DATABASE shard_0;
    DROP DATABASE shard_1;
"
