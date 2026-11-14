#!/usr/bin/env bash
# Tags: zookeeper, no-parallel
# no-parallel: uses the global shard_0 / shard_1 databases of test_cluster_two_shards_different_databases.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

MAP_PARENT="/clickhouse/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}"
MAP_PATH="${MAP_PARENT}/bucket_map"
SKIP_SHARDS="optimize_skip_unused_shards = 1, force_optimize_skip_unused_shards = 1"

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
        found=$($CLICKHOUSE_CLIENT -q "SELECT position(last_exception, '$1') > 0 FROM system.distributed_bucket_maps WHERE database = currentDatabase() AND table = 'dist'")
        if [ "$found" = 1 ]; then
            echo "$1"
            return
        fi
        sleep 0.1
    done
    echo "Timed out waiting for the error '$1' loading the bucket map"
}

function shard_keys()
{
    $CLICKHOUSE_CLIENT -q "SELECT '$1', arraySort(groupArray(key)) FROM $1.data"
}

function loaded_map()
{
    $CLICKHOUSE_CLIENT -q "SELECT version, ranges FROM system.distributed_bucket_maps WHERE database = currentDatabase() AND table = 'dist'"
}

$CLICKHOUSE_CLIENT -m -q "
    DROP DATABASE IF EXISTS shard_0;
    DROP DATABASE IF EXISTS shard_1;
    CREATE DATABASE shard_0;
    CREATE DATABASE shard_1;
    CREATE TABLE shard_0.data (key UInt64) ENGINE = MergeTree ORDER BY key;
    CREATE TABLE shard_1.data (key UInt64) ENGINE = MergeTree ORDER BY key;
"

$CLICKHOUSE_CLIENT --allow_experimental_distributed_bucket_map 1 -q "
    CREATE TABLE dist (key UInt64) ENGINE = Distributed(test_cluster_two_shards_different_databases, '', data, key)
    SETTINGS bucket_map_zookeeper_path = '$MAP_PATH', bucket_map_buckets = 4"

echo '-- buckets 0 and 1 are on shard 1, buckets 2 and 3 on shard 2'
$CLICKHOUSE_CLIENT --distributed_foreground_insert 1 -q "INSERT INTO dist SELECT number FROM numbers(8)"
shard_keys shard_0
shard_keys shard_1

echo '-- without previous shards, the old rows of a bucket assigned to another shard are not found'
set_map 'format version: 1\nbuckets: 4\n0 0 1\n1 3 2\n'
wait_for_map_version 1
$CLICKHOUSE_CLIENT -q "SELECT count() FROM dist WHERE key = 5 SETTINGS $SKIP_SHARDS"

echo '-- with previous shards, they are found'
set_map 'format version: 1\nbuckets: 4\n0 0 1\n1 1 2 previous 1\n2 3 2\n'
wait_for_map_version 2
loaded_map

echo '-- new rows go only to the shard of the bucket'
$CLICKHOUSE_CLIENT --distributed_foreground_insert 1 -q "INSERT INTO dist SELECT number FROM numbers(8, 8)"
shard_keys shard_0
shard_keys shard_1

for prefer_localhost_replica in 0 1; do
    echo "-- shards are skipped by the bucket map with its previous shards, prefer_localhost_replica = $prefer_localhost_replica"
    settings="$SKIP_SHARDS, prefer_localhost_replica = $prefer_localhost_replica"
    $CLICKHOUSE_CLIENT -q "SELECT _shard_num, key FROM dist WHERE key = 5 SETTINGS $settings"
    $CLICKHOUSE_CLIENT -q "SELECT _shard_num, key FROM dist WHERE key = 13 SETTINGS $settings"
    $CLICKHOUSE_CLIENT -q "SELECT _shard_num, key FROM dist WHERE key IN (4, 5, 9, 13) ORDER BY key SETTINGS $settings, optimize_skip_unused_shards_rewrite_in = 0"
    $CLICKHOUSE_CLIENT -q "SELECT _shard_num, key FROM dist WHERE key IN (5, 9) ORDER BY key SETTINGS $settings, optimize_skip_unused_shards_rewrite_in = 1"
    $CLICKHOUSE_CLIENT -q "SELECT _shard_num, key FROM dist WHERE key IN (4, 5, 9, 13) ORDER BY key SETTINGS $settings, optimize_skip_unused_shards_rewrite_in = 1"
done

echo '-- GROUP BY the sharding key merges the rows of a value from all its shards'
$CLICKHOUSE_CLIENT --distributed_foreground_insert 1 -q "INSERT INTO dist VALUES (5)"
shard_keys shard_0
shard_keys shard_1
GROUP_BY_SHARDING_KEY="optimize_skip_unused_shards = 1, optimize_distributed_group_by_sharding_key = 1"
$CLICKHOUSE_CLIENT -q "SELECT key, count() FROM dist WHERE key IN (5, 9) GROUP BY key ORDER BY key SETTINGS $GROUP_BY_SHARDING_KEY"
$CLICKHOUSE_CLIENT -q "SELECT key, count() AS c FROM dist GROUP BY key HAVING c > 1 ORDER BY key SETTINGS $GROUP_BY_SHARDING_KEY"
$CLICKHOUSE_CLIENT -q "SELECT DISTINCT key FROM dist WHERE key = 5 SETTINGS $GROUP_BY_SHARDING_KEY"
$CLICKHOUSE_CLIENT -q "SELECT key FROM dist WHERE key IN (5, 9) ORDER BY key LIMIT 1 BY key SETTINGS $GROUP_BY_SHARDING_KEY"
$CLICKHOUSE_CLIENT -q "SELECT countIf(explain LIKE '%MergingAggregated%') > 0 FROM (EXPLAIN SELECT key, count() FROM dist GROUP BY key SETTINGS $GROUP_BY_SHARDING_KEY)"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM shard_1.data WHERE key = 5"
$CLICKHOUSE_CLIENT -q "ALTER TABLE shard_1.data DELETE WHERE key = 5 SETTINGS mutations_sync = 2"

echo '-- shards that are not previous shards of a bucket are skipped, with a stray row on shard 1'
$CLICKHOUSE_CLIENT -q "INSERT INTO shard_0.data VALUES (6)"
for prefer_localhost_replica in 0 1; do
    settings="$SKIP_SHARDS, prefer_localhost_replica = $prefer_localhost_replica"
    $CLICKHOUSE_CLIENT -q "SELECT _shard_num, key FROM dist WHERE key = 6 SETTINGS $settings"
    $CLICKHOUSE_CLIENT -q "SELECT _shard_num, key FROM dist WHERE key IN (5, 6) ORDER BY _shard_num, key SETTINGS $settings, optimize_skip_unused_shards_rewrite_in = 0"
    $CLICKHOUSE_CLIENT -q "SELECT _shard_num, key FROM dist WHERE key IN (5, 6) ORDER BY _shard_num, key SETTINGS $settings, optimize_skip_unused_shards_rewrite_in = 1"
done
$CLICKHOUSE_CLIENT -q "ALTER TABLE shard_0.data DELETE WHERE key = 6 SETTINGS mutations_sync = 2"

echo '-- a bucket can go back to its previous shard, and previous shards are read only for their buckets'
set_map 'format version: 1\nbuckets: 4\n0 0 2 previous 1\n1 1 1 previous 2\n2 3 2\n'
wait_for_map_version 3
loaded_map
$CLICKHOUSE_CLIENT --distributed_foreground_insert 1 -q "INSERT INTO dist VALUES (16), (17)"
shard_keys shard_0
shard_keys shard_1
$CLICKHOUSE_CLIENT -q "SELECT _shard_num, key FROM dist WHERE key IN (4, 5, 9, 16, 17) ORDER BY key SETTINGS $SKIP_SHARDS, optimize_skip_unused_shards_rewrite_in = 1"

echo '-- GROUP BY the sharding key is finished on the shards again without previous shards'
set_map 'format version: 1\nbuckets: 4\n0 0 1\n1 3 2\n'
wait_for_map_version 4
$CLICKHOUSE_CLIENT -q "SELECT countIf(explain LIKE '%MergingAggregated%') > 0 FROM (EXPLAIN SELECT key, count() FROM dist GROUP BY key SETTINGS $GROUP_BY_SHARDING_KEY)"

echo '-- invalid previous shards are not loaded'
set_map 'format version: 1\nbuckets: 4\n0 0 1\n1 1 2 previous 2\n2 3 2\n'
wait_for_map_exception 'Previous shard 2 of bucket range 1-1 is invalid'
set_map 'format version: 1\nbuckets: 4\n0 0 1\n1 1 2 previous 1 1\n2 3 2\n'
wait_for_map_exception 'Previous shard 1 of bucket range 1-1 is invalid'
set_map 'format version: 1\nbuckets: 4\n0 0 1\n1 1 2 previous 0\n2 3 2\n'
wait_for_map_exception 'Previous shard 0 of bucket range 1-1 is invalid'
set_map 'format version: 1\nbuckets: 4\n0 0 1\n1 1 2 previous\n2 3 2\n'
wait_for_map_exception 'No previous shards are listed for bucket range 1-1'
set_map 'format version: 1\nbuckets: 4\n0 0 1\n1 1 2 previous 1 x\n2 3 2\n'
wait_for_map_exception 'Cannot parse'
set_map 'format version: 1\nbuckets: 4\n0 0 1\n1 1 2 previou\n2 3 2\n'
wait_for_map_exception "expected ''previous''"
loaded_map

echo '-- a bucket can have several previous shards, but not a shard that the cluster does not have'
set_map 'format version: 1\nbuckets: 4\n0 0 1\n1 1 2 previous 3 1\n2 3 2\n'
wait_for_map_version 11
loaded_map
$CLICKHOUSE_CLIENT -q "SELECT count() FROM dist WHERE key = 5 SETTINGS $SKIP_SHARDS" 2>&1 \
    | grep -o -m1 'assigns buckets to shard 3, but cluster test_cluster_two_shards_different_databases has 2 shards'

echo '-- a query that does not skip shards does not need the map'
$CLICKHOUSE_CLIENT -q "DETACH TABLE dist"
set_map 'format version: 1\nbuckets: 4\n0 0 1\n'
$CLICKHOUSE_CLIENT -q "ATTACH TABLE dist"
wait_for_map_exception 'The bucket ranges cover 1 of 4 buckets'
$CLICKHOUSE_CLIENT -q "SELECT count() FROM dist SETTINGS optimize_skip_unused_shards = 1"

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE dist;
    DROP DATABASE shard_0;
    DROP DATABASE shard_1;
"
