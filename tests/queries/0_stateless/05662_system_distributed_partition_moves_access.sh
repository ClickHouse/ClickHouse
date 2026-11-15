#!/usr/bin/env bash
# Tags: zookeeper

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

MOVES_PATH="/clickhouse/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/partition_moves"
TEST_USER="user_${CLICKHOUSE_DATABASE}"
MOVES_OF_DATABASE="FROM system.distributed_partition_moves WHERE database = '${CLICKHOUSE_DATABASE}'"

$CLICKHOUSE_CLIENT --allow_experimental_distributed_partition_moves 1 -m -q "
    CREATE TABLE visible (key UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), data, key)
        SETTINGS partition_moves_zookeeper_path = '$MOVES_PATH';
    CREATE TABLE hidden (key UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), data, key)
        SETTINGS partition_moves_zookeeper_path = '$MOVES_PATH';

    DROP USER IF EXISTS $TEST_USER;
    CREATE USER $TEST_USER;
    GRANT SELECT ON system.distributed_partition_moves TO $TEST_USER;
"

echo '-- the tables have partition moves'
$CLICKHOUSE_CLIENT -q "SELECT table $MOVES_OF_DATABASE ORDER BY table"

echo '-- a user without access to the tables does not see them'
$CLICKHOUSE_CLIENT --user "$TEST_USER" -q "SELECT count() FROM system.distributed_partition_moves"
$CLICKHOUSE_CLIENT --user "$TEST_USER" -q "SELECT count() $MOVES_OF_DATABASE"
$CLICKHOUSE_CLIENT --user "$TEST_USER" -q "SELECT count() $MOVES_OF_DATABASE AND table = 'visible'"

echo '-- SHOW TABLES on one table shows only this table'
$CLICKHOUSE_CLIENT -q "GRANT SHOW TABLES ON ${CLICKHOUSE_DATABASE}.visible TO $TEST_USER"
$CLICKHOUSE_CLIENT --user "$TEST_USER" -q "SELECT table FROM system.distributed_partition_moves"
$CLICKHOUSE_CLIENT --user "$TEST_USER" -q "SELECT table $MOVES_OF_DATABASE"
$CLICKHOUSE_CLIENT --user "$TEST_USER" -q "SELECT table $MOVES_OF_DATABASE AND table = 'visible'"
$CLICKHOUSE_CLIENT --user "$TEST_USER" -q "SELECT count() $MOVES_OF_DATABASE AND table = 'hidden'"

$CLICKHOUSE_CLIENT -m -q "
    DROP USER $TEST_USER;
    DROP TABLE visible;
    DROP TABLE hidden;
"
