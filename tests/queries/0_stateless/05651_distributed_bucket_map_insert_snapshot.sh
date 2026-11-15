#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-fasttest
# no-parallel: uses the global shard_0 / shard_1 databases of test_cluster_two_shards_different_databases
# and a server-wide failpoint. no-fasttest: tests that run alone are kept out of the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

MAP_PARENT="/clickhouse/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}"
FAILPOINT="distributed_sink_pause_in_constructor"

trap '$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FAILPOINT" 2>/dev/null' EXIT

function wait_for_map_version()
{
    for _ in {1..600}; do
        version=$($CLICKHOUSE_CLIENT -q "SELECT version FROM system.distributed_bucket_maps WHERE database = currentDatabase() AND table = 'dist'")
        [ "$version" = "$1" ] && return
        sleep 0.1
    done
    echo "Timed out waiting for version $1 of the bucket map, the loaded version is $version"
}

function shard_keys()
{
    $CLICKHOUSE_CLIENT -q "SELECT '$1', arraySort(groupArray(key)) FROM $1.data"
}

$CLICKHOUSE_CLIENT --allow_experimental_distributed_bucket_map 1 -m -q "
    DROP DATABASE IF EXISTS shard_0;
    DROP DATABASE IF EXISTS shard_1;
    CREATE DATABASE shard_0;
    CREATE DATABASE shard_1;
    CREATE TABLE shard_0.data (key UInt64) ENGINE = MergeTree ORDER BY key;
    CREATE TABLE shard_1.data (key UInt64) ENGINE = MergeTree ORDER BY key;

    CREATE TABLE dist (key UInt64) ENGINE = Distributed(test_cluster_two_shards_different_databases, '', data, key)
        SETTINGS bucket_map_zookeeper_path = '${MAP_PARENT}/bucket_map', bucket_map_buckets = 4;

    -- One INSERT into the source table makes two sinks of the Distributed table.
    CREATE TABLE source (key UInt64) ENGINE = Null;
    CREATE MATERIALIZED VIEW view_1 TO dist AS SELECT key FROM source;
    CREATE MATERIALIZED VIEW view_2 TO dist AS SELECT key + 100 AS key FROM source;
"

echo '-- the map changes after the first sink of the INSERT is created, both sinks use the old map'
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FAILPOINT"
$CLICKHOUSE_CLIENT --distributed_foreground_insert 1 -q "INSERT INTO source SELECT number FROM numbers(4)" &
timeout 60 $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $FAILPOINT PAUSE"
$CLICKHOUSE_CLIENT -q "INSERT INTO system.zookeeper (name, path, value) VALUES ('bucket_map', '$MAP_PARENT', 'format version: 1\nbuckets: 4\n0 1 2\n2 3 1\n')"
wait_for_map_version 1
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FAILPOINT"
wait
shard_keys shard_0
shard_keys shard_1

echo '-- the next INSERT uses the new map'
$CLICKHOUSE_CLIENT --distributed_foreground_insert 1 -q "INSERT INTO source VALUES (4), (6)"
shard_keys shard_0
shard_keys shard_1

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE view_1;
    DROP TABLE view_2;
    DROP TABLE source;
    DROP TABLE dist;
    DROP DATABASE shard_0;
    DROP DATABASE shard_1;
"
