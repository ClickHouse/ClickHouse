#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $user"
$CLICKHOUSE_CLIENT -q "CREATE USER $user IDENTIFIED WITH no_password"
$CLICKHOUSE_CLIENT -q "GRANT UNDROP TABLE ON ${CLICKHOUSE_DATABASE}.* TO $user"

$CLICKHOUSE_CLIENT -q "CREATE TABLE t1 (n UInt64) ENGINE = MergeTree ORDER BY n"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t2 (n UInt64) ENGINE = MergeTree ORDER BY n"
$CLICKHOUSE_CLIENT -q "INSERT INTO t1 VALUES (1)"
$CLICKHOUSE_CLIENT -q "INSERT INTO t2 VALUES (2)"
$CLICKHOUSE_CLIENT -q "DROP TABLE t1 SETTINGS database_atomic_wait_for_drop_and_detach_synchronously = 0"
$CLICKHOUSE_CLIENT -q "DROP TABLE t2 SETTINGS database_atomic_wait_for_drop_and_detach_synchronously = 0"

$CLICKHOUSE_CLIENT --user "$user" -q "UNDROP TABLE ${CLICKHOUSE_DATABASE}.t1"
$CLICKHOUSE_CLIENT --user "$user" -q "UNDROP TABLE t2"
$CLICKHOUSE_CLIENT -q "SELECT * FROM t1"
$CLICKHOUSE_CLIENT -q "SELECT * FROM t2"

$CLICKHOUSE_CLIENT -q "REVOKE UNDROP TABLE ON ${CLICKHOUSE_DATABASE}.* FROM $user"
$CLICKHOUSE_CLIENT -q "GRANT UNDROP TABLE ON ${CLICKHOUSE_DATABASE}.t2 TO $user"
uuid=$($CLICKHOUSE_CLIENT -q "SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 't1'")
$CLICKHOUSE_CLIENT -q "DROP TABLE t1 SETTINGS database_atomic_wait_for_drop_and_detach_synchronously = 0"
$CLICKHOUSE_CLIENT -q "DROP TABLE t2 SETTINGS database_atomic_wait_for_drop_and_detach_synchronously = 0"
$CLICKHOUSE_CLIENT --user "$user" -q "UNDROP TABLE t1" 2>&1 | grep -Fq "ACCESS_DENIED" && echo OK
$CLICKHOUSE_CLIENT --user "$user" -q "UNDROP TABLE t2 UUID '$uuid'" 2>&1 | grep -Fq "UNKNOWN_TABLE" && echo OK
$CLICKHOUSE_CLIENT -q "UNDROP TABLE t1"
$CLICKHOUSE_CLIENT -q "SELECT * FROM t1"

$CLICKHOUSE_CLIENT -q "DROP USER $user"
