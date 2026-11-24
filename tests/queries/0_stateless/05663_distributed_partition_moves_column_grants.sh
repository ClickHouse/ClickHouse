#!/usr/bin/env bash
# Tags: zookeeper

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

MOVES_PARENT="/clickhouse/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}"
MOVES_PATH="${MOVES_PARENT}/partition_moves"
TEST_USER="user_${CLICKHOUSE_DATABASE}"

# Both shards of the cluster are local and read the same table.
$CLICKHOUSE_CLIENT --allow_experimental_distributed_partition_moves 1 -m -q "
    CREATE TABLE data (d Date, x UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY x;
    INSERT INTO data SELECT '2024-01-15', number FROM numbers(1, 3);
    INSERT INTO data SELECT '2024-02-15', number FROM numbers(10, 3);
    CREATE TABLE dist (d Date, x UInt64) ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), data)
        SETTINGS partition_moves_zookeeper_path = '$MOVES_PATH';

    DROP USER IF EXISTS $TEST_USER;
    CREATE USER $TEST_USER;
    GRANT SELECT(x) ON ${CLICKHOUSE_DATABASE}.dist TO $TEST_USER;
    GRANT SELECT(x) ON ${CLICKHOUSE_DATABASE}.data TO $TEST_USER;
"

echo '-- a user with a grant on one column reads the table without a move'
$CLICKHOUSE_CLIENT --user "$TEST_USER" --prefer_localhost_replica 1 -q "SELECT sum(x) FROM dist"

$CLICKHOUSE_CLIENT -q "INSERT INTO system.zookeeper (name, path, value) VALUES ('partition_moves', '$MOVES_PARENT', 'format version: 1\n202401 1 2 source\n')"
for _ in {1..600}; do
    version=$($CLICKHOUSE_CLIENT -q "SELECT version FROM system.distributed_partition_moves WHERE database = currentDatabase() AND table = 'dist'")
    [ "$version" = "1" ] && break
    sleep 0.1
done

echo '-- and reads it while the second shard skips a partition'
$CLICKHOUSE_CLIENT --user "$TEST_USER" --prefer_localhost_replica 1 -q "SELECT sum(x) FROM dist"
$CLICKHOUSE_CLIENT --user "$TEST_USER" --prefer_localhost_replica 1 -q "SELECT count() FROM dist"

echo '-- a query that reads _partition_id of another table does not need the grant on the local table'
$CLICKHOUSE_CLIENT -m -q "
    CREATE TABLE other (d Date, x UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY x;
    INSERT INTO other SELECT '2024-01-15', number FROM numbers(1, 3);
    INSERT INTO other SELECT '2024-02-15', number FROM numbers(10, 3);
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.other TO $TEST_USER;
"
$CLICKHOUSE_CLIENT --user "$TEST_USER" --prefer_localhost_replica 1 -q "SELECT sum(x) FROM dist WHERE x IN (SELECT x FROM other WHERE _partition_id = '202401')"
$CLICKHOUSE_CLIENT --user "$TEST_USER" --prefer_localhost_replica 1 -q "SELECT sum(x) FROM dist AS l INNER JOIN other AS r USING (x) WHERE r._partition_id = '202401'"

echo '-- a query that reads _partition_id itself needs the grant on the local table'
$CLICKHOUSE_CLIENT -q "GRANT SELECT(_partition_id) ON ${CLICKHOUSE_DATABASE}.dist TO $TEST_USER"
$CLICKHOUSE_CLIENT --user "$TEST_USER" --prefer_localhost_replica 1 -q "SELECT sum(x) FROM dist WHERE _partition_id = '202402'" 2>&1 \
    | grep -o -m1 'ACCESS_DENIED'
$CLICKHOUSE_CLIENT -q "GRANT SELECT(_partition_id) ON ${CLICKHOUSE_DATABASE}.data TO $TEST_USER"
$CLICKHOUSE_CLIENT --user "$TEST_USER" --prefer_localhost_replica 1 -q "SELECT sum(x) FROM dist WHERE _partition_id = '202402'"

$CLICKHOUSE_CLIENT -m -q "
    DROP USER $TEST_USER;
    DROP TABLE dist;
    DROP TABLE other;
    DROP TABLE data;
"
