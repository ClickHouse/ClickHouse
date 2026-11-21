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

echo '-- a table with a bucket map needs the number of buckets'
$CLICKHOUSE_CLIENT --allow_experimental_distributed_bucket_map 1 -q "CREATE TABLE dist $DIST_DEFINITION SETTINGS bucket_map_zookeeper_path = '$MAP_PATH'" 2>&1 \
    | grep -o -m1 'requires `bucket_map_buckets`'

echo '-- a new map splits the buckets evenly between the shards'
$CLICKHOUSE_CLIENT --allow_experimental_distributed_bucket_map 1 -q "CREATE TABLE dist $DIST_DEFINITION SETTINGS bucket_map_zookeeper_path = '$MAP_PATH', bucket_map_buckets = 4"
$CLICKHOUSE_CLIENT -q "SELECT value FROM system.zookeeper WHERE path = '$MAP_PARENT' AND name = 'bucket_map' FORMAT TSVRaw"

echo '-- table functions do not take a bucket map and do not create it'
for allow_experimental in 0 1; do
    $CLICKHOUSE_CLIENT --allow_experimental_distributed_bucket_map $allow_experimental -q "SELECT * FROM cluster(test_cluster_two_shards_different_databases, shard_0.data, key, SETTINGS bucket_map_zookeeper_path = '${MAP_PARENT}/function_bucket_map', bucket_map_buckets = 4)" 2>&1 \
        | grep -o -m1 'is not supported by table function'
    $CLICKHOUSE_CLIENT --allow_experimental_distributed_bucket_map $allow_experimental -q "SELECT * FROM remote('127.0.0.{1,2}', shard_0.data, key, SETTINGS bucket_map_zookeeper_path = '${MAP_PARENT}/function_bucket_map', bucket_map_buckets = 4)" 2>&1 \
        | grep -o -m1 'is not supported by table function'
done
$CLICKHOUSE_CLIENT -q "SELECT name FROM system.zookeeper WHERE path = '$MAP_PARENT' ORDER BY name"

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

echo '-- a map with a different number of buckets is rejected, the loaded map keeps routing'
set_map 'format version: 1\nbuckets: 4\n0 1 1\n2 3 2\n'
wait_for_map_version 4
set_map 'format version: 1\nbuckets: 8\n0 3 1\n4 7 2\n'
wait_for_map_exception
$CLICKHOUSE_CLIENT -q "SELECT last_exception LIKE '%The map has 8 buckets, but the table uses 4 buckets%' FROM system.distributed_bucket_maps WHERE database = currentDatabase() AND table = 'dist'"
$CLICKHOUSE_CLIENT -q "SELECT version, buckets, ranges FROM system.distributed_bucket_maps WHERE database = currentDatabase() AND table = 'dist'"
$CLICKHOUSE_CLIENT --distributed_foreground_insert 1 -q "INSERT INTO dist VALUES (101)"
shard_keys shard_0
set_map 'format version: 1\nbuckets: 4\n0 0 1\n1 3 2\n'
wait_for_map_version 6
$CLICKHOUSE_CLIENT -q "SELECT version, buckets, ranges, last_exception FROM system.distributed_bucket_maps WHERE database = currentDatabase() AND table = 'dist'"

echo '-- the table is attached without the experimental setting'
set_map 'format version: 1\nbuckets: 4\n0 1 1\n2 3 2\n'
$CLICKHOUSE_CLIENT -q "DETACH TABLE dist"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE dist"
$CLICKHOUSE_CLIENT --distributed_foreground_insert 1 -q "INSERT INTO dist VALUES (102)"
shard_keys shard_1

echo '-- a map with a different number of buckets is rejected after the table is attached again'
set_map 'format version: 1\nbuckets: 8\n0 3 1\n4 7 2\n'
wait_for_map_exception
$CLICKHOUSE_CLIENT -q "SELECT last_exception LIKE '%The map has 8 buckets, but the table uses 4 buckets%', buckets FROM system.distributed_bucket_maps WHERE database = currentDatabase() AND table = 'dist'"

echo '-- INSERT SELECT from a Distributed table of the same cluster goes through the bucket map'
$CLICKHOUSE_CLIENT -m -q "
    CREATE TABLE shard_0.source_data (key UInt64) ENGINE = MergeTree ORDER BY key;
    CREATE TABLE shard_1.source_data (key UInt64) ENGINE = MergeTree ORDER BY key;
    CREATE TABLE source_dist (key UInt64) ENGINE = Distributed(test_cluster_two_shards_different_databases, '', source_data, key);
    INSERT INTO shard_1.source_data VALUES (201);
"
$CLICKHOUSE_CLIENT --distributed_foreground_insert 1 -q "INSERT INTO dist SELECT key FROM source_dist"
shard_keys shard_0
$CLICKHOUSE_CLIENT -q "SELECT count() FROM dist WHERE key = 201 SETTINGS optimize_skip_unused_shards = 1, force_optimize_skip_unused_shards = 1"

echo '-- INSERT SELECT from a table with a bucket map into an ordinary Distributed table goes through the shard weights'
$CLICKHOUSE_CLIENT -m -q "
    CREATE TABLE shard_0.plain_data (key UInt64) ENGINE = MergeTree ORDER BY key;
    CREATE TABLE shard_1.plain_data (key UInt64) ENGINE = MergeTree ORDER BY key;
    CREATE TABLE plain_dist (key UInt64) ENGINE = Distributed(test_cluster_two_shards_different_databases, '', plain_data, key);
"
$CLICKHOUSE_CLIENT --distributed_foreground_insert 1 --parallel_distributed_insert_select 2 -q "INSERT INTO plain_dist SELECT key FROM dist WHERE key = 201"
$CLICKHOUSE_CLIENT -q "SELECT 'shard_1', arraySort(groupArray(key)) FROM shard_1.plain_data"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM plain_dist WHERE key = 201 SETTINGS optimize_skip_unused_shards = 1, force_optimize_skip_unused_shards = 1"

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE plain_dist;
    DROP TABLE source_dist;
    DROP TABLE dist;
    DROP DATABASE shard_0;
    DROP DATABASE shard_1;
"
