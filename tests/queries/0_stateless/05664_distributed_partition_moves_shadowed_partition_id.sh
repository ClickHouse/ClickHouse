#!/usr/bin/env bash
# Tags: zookeeper

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

MOVES_PARENT="/clickhouse/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}"
MOVES_PATH="${MOVES_PARENT}/partition_moves"

function read_dist()
{
    for prefer_localhost_replica in 0 1; do
        for serialize_query_plan in 0 1; do
            $CLICKHOUSE_CLIENT --prefer_localhost_replica $prefer_localhost_replica --serialize_query_plan $serialize_query_plan \
                -q "SELECT count() FROM dist" 2>&1 | grep -o -m1 -E '^[0-9]+$|hides the virtual column'
        done
    done
}

# Both shards of the cluster are local and read the same table, whose column `_partition_id` hides the virtual column.
$CLICKHOUSE_CLIENT --allow_experimental_distributed_partition_moves 1 --allow_reserved_column_names 1 -m -q "
    CREATE TABLE data (p UInt8, x UInt64, _partition_id String) ENGINE = MergeTree PARTITION BY p ORDER BY x;
    INSERT INTO data VALUES (1, 1, 'application-id');
    CREATE TABLE dist (p UInt8, x UInt64) ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), data)
        SETTINGS partition_moves_zookeeper_path = '$MOVES_PATH';
"

echo '-- without a move both shards read the partition'
read_dist

$CLICKHOUSE_CLIENT -q "INSERT INTO system.zookeeper (name, path, value) VALUES ('partition_moves', '$MOVES_PARENT', 'format version: 1\n1 1 2 destination\n')"
for _ in {1..600}; do
    version=$($CLICKHOUSE_CLIENT -q "SELECT version FROM system.distributed_partition_moves WHERE database = currentDatabase() AND table = 'dist'")
    [ "$version" = "1" ] && break
    sleep 0.1
done

echo '-- the first shard does not skip the partition by the values of the column'
read_dist
$CLICKHOUSE_CLIENT -q "SELECT _partition_id FROM data"

echo '-- and skips it by the virtual column when the column is dropped'
$CLICKHOUSE_CLIENT -q "ALTER TABLE data DROP COLUMN _partition_id"
read_dist

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE dist;
    DROP TABLE data;
"
