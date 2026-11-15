#!/usr/bin/env bash
# Tags: zookeeper, no-parallel
# no-parallel: uses the global shard_0 / shard_1 databases of test_cluster_two_shards_different_databases.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

MAP_PARENT="/clickhouse/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}"
KEYS="-128 -127 -100 -7 -3 -2 -1 0 1 2 3 7 100 127"

$CLICKHOUSE_CLIENT -m -q "
    DROP DATABASE IF EXISTS shard_0;
    DROP DATABASE IF EXISTS shard_1;
    CREATE DATABASE shard_0;
    CREATE DATABASE shard_1;
"

# 7 buckets are not a power of two, and 512 buckets are more than the values of Int8.
for buckets in 7 512; do
    for type in Int8 Int16 Int32 Int64; do
        echo "-- $type, $buckets buckets"
        $CLICKHOUSE_CLIENT --allow_experimental_distributed_bucket_map 1 -m -q "
            CREATE TABLE shard_0.data (key $type) ENGINE = MergeTree ORDER BY key;
            CREATE TABLE shard_1.data (key $type) ENGINE = MergeTree ORDER BY key;
            CREATE TABLE dist (key $type) ENGINE = Distributed(test_cluster_two_shards_different_databases, '', data, key)
                SETTINGS bucket_map_zookeeper_path = '${MAP_PARENT}/bucket_map_${buckets}', bucket_map_buckets = $buckets;
        "
        $CLICKHOUSE_CLIENT --distributed_foreground_insert 1 -q "INSERT INTO dist VALUES (${KEYS// /), (})"

        echo 'rows on a shard that is not the shard of the bucket'
        $CLICKHOUSE_CLIENT -q "
            SELECT (SELECT count() FROM shard_0.data WHERE reinterpretAsUInt64(toInt64(key)) % $buckets >= intDiv($buckets, 2))
                 + (SELECT count() FROM shard_1.data WHERE reinterpretAsUInt64(toInt64(key)) % $buckets < intDiv($buckets, 2))"

        echo 'key = value'
        for key in $KEYS; do
            echo "SELECT key, _shard_num FROM dist WHERE key = $key SETTINGS optimize_skip_unused_shards = 1, force_optimize_skip_unused_shards = 1;"
        done | $CLICKHOUSE_CLIENT -m

        echo 'key IN (values)'
        $CLICKHOUSE_CLIENT -q "
            SELECT key, _shard_num FROM dist WHERE key IN (${KEYS// /, }) ORDER BY key
            SETTINGS optimize_skip_unused_shards = 1, force_optimize_skip_unused_shards = 1, optimize_skip_unused_shards_rewrite_in = 1"
        $CLICKHOUSE_CLIENT -q "
            SELECT key, _shard_num FROM dist WHERE key IN (-128, -3, -1, 1, 3, 127) ORDER BY key
            SETTINGS optimize_skip_unused_shards = 1, force_optimize_skip_unused_shards = 1, optimize_skip_unused_shards_rewrite_in = 1"

        $CLICKHOUSE_CLIENT -m -q "
            DROP TABLE dist;
            DROP TABLE shard_0.data;
            DROP TABLE shard_1.data;
        "
    done
done

$CLICKHOUSE_CLIENT -m -q "
    DROP DATABASE shard_0;
    DROP DATABASE shard_1;
"
