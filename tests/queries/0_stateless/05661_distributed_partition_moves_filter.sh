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
        version=$($CLICKHOUSE_CLIENT -q "SELECT version FROM system.distributed_partition_moves WHERE database = currentDatabase() AND table = '${2:-dist}'")
        [ "$version" = "$1" ] && return
        sleep 0.1
    done
    echo "Timed out waiting for version $1 of the partition moves, the loaded version is $version"
}

$CLICKHOUSE_CLIENT -m -q "
    DROP DATABASE IF EXISTS shard_0;
    DROP DATABASE IF EXISTS shard_1;
    CREATE DATABASE shard_0;
    CREATE DATABASE shard_1;
    CREATE TABLE shard_0.data (d Date, x UInt64) ENGINE = ReplacingMergeTree PARTITION BY toYYYYMM(d) ORDER BY x;
    CREATE TABLE shard_1.data (d Date, x UInt64) ENGINE = ReplacingMergeTree PARTITION BY toYYYYMM(d) ORDER BY x;
    INSERT INTO shard_0.data SELECT '2024-01-15', number FROM numbers(1, 3);
    INSERT INTO shard_1.data SELECT '2024-02-15', number FROM numbers(10, 3);
"
$CLICKHOUSE_CLIENT --allow_experimental_distributed_partition_moves 1 -q "
    CREATE TABLE dist (d Date, x UInt64) ENGINE = Distributed(test_cluster_two_shards_different_databases, '', data)
    SETTINGS partition_moves_zookeeper_path = '$MOVES_PATH'"

# Both shards have the partition 202401, with and without a move.
$CLICKHOUSE_CLIENT -q "ALTER TABLE shard_1.data ATTACH PARTITION ID '202401' FROM shard_0.data"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM dist"

version=0
for read_from in source destination; do
    echo "-- a read-only user reads one copy from the $read_from, from local and from remote shards"
    set_moves "format version: 1\n202401 1 2 $read_from\n"
    version=$((version + 1))
    wait_for_version $version
    for prefer_localhost_replica in 0 1; do
        $CLICKHOUSE_CLIENT --prefer_localhost_replica $prefer_localhost_replica --readonly 1 -q "SELECT count(), sum(x) FROM dist"
        $CLICKHOUSE_CLIENT --prefer_localhost_replica $prefer_localhost_replica --readonly 1 -q "SELECT count() FROM dist WHERE x != 1 OR x = 10"
        $CLICKHOUSE_CLIENT --prefer_localhost_replica $prefer_localhost_replica --readonly 1 -q "SELECT _shard_num, count() FROM dist GROUP BY 1 ORDER BY 1"
    done
done

echo '-- the copy is skipped before a join that keeps the rows of the left table'
JOINED="(SELECT number AS x FROM numbers(1, 2)) AS r USING (x)"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM dist AS l INNER JOIN $JOINED"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM dist AS l LEFT JOIN $JOINED"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM dist AS l RIGHT JOIN $JOINED" 2>&1 \
    | grep -o -m1 'This JOIN is not supported for table'

echo '-- the copy is skipped wherever the table is in the query for the shard'
$CLICKHOUSE_CLIENT -q "CREATE VIEW v AS SELECT * FROM dist"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(x) FROM v"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(x) FROM v WHERE x != 1"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(x) FROM (SELECT x FROM dist)"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(x) FROM dist FINAL"
$CLICKHOUSE_CLIENT -q "SELECT count(), sum(t.x) FROM dist AS t WHERE t.x > 0"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM (SELECT number AS x FROM numbers(1, 2)) AS l INNER JOIN dist AS r USING (x)"
for distributed_product_mode in local global; do
    $CLICKHOUSE_CLIENT --distributed_product_mode $distributed_product_mode -q "SELECT count() FROM dist AS a INNER JOIN dist AS b USING (x)"
    $CLICKHOUSE_CLIENT --distributed_product_mode $distributed_product_mode -q "SELECT count() FROM dist WHERE x IN (SELECT x FROM dist WHERE x < 11)"
done

echo '-- a query that also reads the remote table directly fails'
$CLICKHOUSE_CLIENT -m -q "
    CREATE TABLE local_data (d Date, x UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY x;
    INSERT INTO local_data SELECT '2024-01-15', number FROM numbers(1, 3);
    INSERT INTO local_data SELECT '2024-02-15', number FROM numbers(10, 2);
"
$CLICKHOUSE_CLIENT --allow_experimental_distributed_partition_moves 1 -q "
    CREATE TABLE dist_same (d Date, x UInt64) ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), local_data)
    SETTINGS partition_moves_zookeeper_path = '$MOVES_PATH'"
wait_for_version $version dist_same
$CLICKHOUSE_CLIENT -q "SELECT count() FROM dist_same"
$CLICKHOUSE_CLIENT --distributed_product_mode local -q "SELECT count() FROM dist_same WHERE x IN (SELECT x FROM dist_same)"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM dist_same WHERE x IN (SELECT x FROM ${CLICKHOUSE_DATABASE}.local_data)" 2>&1 \
    | grep -o -m1 'is read directly by a query'
$CLICKHOUSE_CLIENT -q "SELECT count() FROM dist_same AS l INNER JOIN ${CLICKHOUSE_DATABASE}.local_data AS r USING (x)" 2>&1 \
    | grep -o -m1 'is read directly by a query'

echo '-- a table as the right argument of IN and another Distributed table replaced by its local table fail'
$CLICKHOUSE_CLIENT -m -q "
    CREATE TABLE other_data (d Date, x UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY x;
    INSERT INTO other_data SELECT '2024-01-15', number FROM numbers(1, 3);
    CREATE TABLE plain_same (d Date, x UInt64) ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), local_data);
    CREATE TABLE plain_other (d Date, x UInt64) ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), other_data);
"
$CLICKHOUSE_CLIENT --distributed_product_mode local -q "SELECT count() FROM dist_same WHERE (d, x) IN dist_same" 2>&1 \
    | grep -o -m1 'NOT_IMPLEMENTED'
# A direct read and a table without FROM together keep the numbers of tables equal.
for in_operator in 'IN' 'NOT IN'; do
    $CLICKHOUSE_CLIENT --distributed_product_mode local -q "
        SELECT count() FROM dist_same
        WHERE (d, x) $in_operator dist_same AND x IN (SELECT x FROM ${CLICKHOUSE_DATABASE}.local_data)" 2>&1 \
        | grep -o -m1 'NOT_IMPLEMENTED'
done
$CLICKHOUSE_CLIENT --distributed_product_mode local -q "SELECT count() FROM dist_same WHERE x IN (SELECT x FROM plain_same)" 2>&1 \
    | grep -o -m1 'NOT_IMPLEMENTED'
$CLICKHOUSE_CLIENT --distributed_product_mode local -q "SELECT count() FROM plain_other AS a INNER JOIN dist_same AS b USING (x)" 2>&1 \
    | grep -o -m1 'NOT_IMPLEMENTED'
$CLICKHOUSE_CLIENT --distributed_product_mode global -q "SELECT count() FROM dist_same WHERE x IN (SELECT x FROM plain_same)"

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE plain_same;
    DROP TABLE plain_other;
    DROP TABLE other_data;
    DROP TABLE dist_same;
    DROP TABLE local_data;
    DROP VIEW v;
    DROP TABLE dist;
    DROP DATABASE shard_0;
    DROP DATABASE shard_1;
"
