#!/usr/bin/env bash
# Tags: zookeeper, no-parallel
# no-parallel: uses the global shard_0 / shard_1 databases of test_cluster_two_shards_different_databases_zero_weight.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

MAP_PATH="/clickhouse/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/bucket_map"

$CLICKHOUSE_CLIENT -m -q "
    DROP DATABASE IF EXISTS shard_0;
    DROP DATABASE IF EXISTS shard_1;
    CREATE DATABASE shard_0;
    CREATE DATABASE shard_1;
    CREATE TABLE shard_0.data (key UInt64) ENGINE = MergeTree ORDER BY key;
    CREATE TABLE shard_1.data (key UInt64) ENGINE = MergeTree ORDER BY key;
"

$CLICKHOUSE_CLIENT --allow_experimental_distributed_bucket_map 1 -q "
    CREATE TABLE dist (key UInt64) ENGINE = Distributed(test_cluster_two_shards_different_databases_zero_weight, '', data, key)
    SETTINGS bucket_map_zookeeper_path = '$MAP_PATH', bucket_map_buckets = 4"

echo '-- the buckets of a shard with zero weight take rows'
$CLICKHOUSE_CLIENT --distributed_foreground_insert 1 -q "INSERT INTO dist SELECT number FROM numbers(8)"
$CLICKHOUSE_CLIENT -q "SELECT 'shard_0', arraySort(groupArray(key)) FROM shard_0.data"
$CLICKHOUSE_CLIENT -q "SELECT 'shard_1', arraySort(groupArray(key)) FROM shard_1.data"

echo '-- the shard with zero weight is read when it is the only shard left'
$CLICKHOUSE_CLIENT -q "SELECT _shard_num, key FROM dist WHERE key = 2 SETTINGS optimize_skip_unused_shards = 1, force_optimize_skip_unused_shards = 1"
$CLICKHOUSE_CLIENT -q "SELECT _shard_num, key FROM dist WHERE key IN (2, 7) ORDER BY key SETTINGS optimize_skip_unused_shards = 1, optimize_skip_unused_shards_rewrite_in = 1, force_optimize_skip_unused_shards = 1"
$CLICKHOUSE_CLIENT -q "SELECT _shard_num, key FROM dist WHERE key IN (1, 2) ORDER BY key SETTINGS optimize_skip_unused_shards = 1, optimize_skip_unused_shards_rewrite_in = 1, force_optimize_skip_unused_shards = 1"

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE dist;
    DROP DATABASE shard_0;
    DROP DATABASE shard_1;
"
