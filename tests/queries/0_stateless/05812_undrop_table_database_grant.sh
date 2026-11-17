#!/usr/bin/env bash
# Tags: zookeeper, no-ordinary-database, no-replicated-database

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
$CLICKHOUSE_CLIENT --user "$user" -q "UNDROP TABLE t2 UUID '$uuid'" 2>&1 | grep -Fq "is being dropped, has been dropped" && echo OK
$CLICKHOUSE_CLIENT -q "GRANT CLUSTER ON *.* TO $user"
$CLICKHOUSE_CLIENT --user "$user" -q "UNDROP TABLE t2 UUID '$uuid' ON CLUSTER test_shard_localhost" 2>&1 | grep -Fq "ACCESS_DENIED" && echo OK
$CLICKHOUSE_CLIENT --user "$user" --readonly 1 -q "UNDROP TABLE t2" 2>&1 | grep -Fq "READONLY" && echo OK
$CLICKHOUSE_CLIENT --user "$user" --allow_ddl 0 -q "UNDROP TABLE t2" 2>&1 | grep -Fq "QUERY_IS_PROHIBITED" && echo OK
$CLICKHOUSE_CLIENT -q "UNDROP TABLE t1"
$CLICKHOUSE_CLIENT -q "SELECT * FROM t1"

$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${CLICKHOUSE_DATABASE}_other ENGINE = Atomic"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${CLICKHOUSE_DATABASE}_other.t2 (n UInt64) ENGINE = MergeTree ORDER BY n"
uuid=$($CLICKHOUSE_CLIENT -q "SELECT uuid FROM system.tables WHERE database = '${CLICKHOUSE_DATABASE}_other' AND name = 't2'")
$CLICKHOUSE_CLIENT -q "DROP TABLE ${CLICKHOUSE_DATABASE}_other.t2 SETTINGS database_atomic_wait_for_drop_and_detach_synchronously = 0"
$CLICKHOUSE_CLIENT --user "$user" -q "UNDROP TABLE t2 UUID '$uuid'" 2>&1 | grep -Fq "is being dropped, has been dropped" && echo OK
$CLICKHOUSE_CLIENT -q "UNDROP TABLE t2"
$CLICKHOUSE_CLIENT -q "SELECT * FROM t2"

session="${CLICKHOUSE_DATABASE}_05812"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${session}" -d "CREATE TEMPORARY TABLE tmp (n UInt64)"
tmp_uuid=$(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&session_id=${session}" -d "SELECT uuid FROM system.tables WHERE is_temporary AND name = 'tmp'")
$CLICKHOUSE_CLIENT --user "$user" -q "UNDROP TABLE _temporary_and_external_tables.\`_tmp_${tmp_uuid}\`" 2>&1 | grep -Fq "(DATABASE_ACCESS_DENIED)" && echo OK
$CLICKHOUSE_CLIENT --user "$user" -q "UNDROP TABLE _temporary_and_external_tables.\`_tmp_00000000-0000-0000-0000-000000000000\`" 2>&1 | grep -Fq "(DATABASE_ACCESS_DENIED)" && echo OK

$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${CLICKHOUSE_DATABASE}_rep ENGINE = Replicated('/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/05812_rep', '1', '1')"
$CLICKHOUSE_CLIENT --user "$user" -q "UNDROP TABLE ${CLICKHOUSE_DATABASE}_rep.t ON CLUSTER ${CLICKHOUSE_DATABASE}_rep" 2>&1 | grep -Fq "(ACCESS_DENIED)" && echo OK
$CLICKHOUSE_CLIENT --user "$user" -q "UNDROP TABLE ${CLICKHOUSE_DATABASE}_absent.t ON CLUSTER ${CLICKHOUSE_DATABASE}_absent" 2>&1 | grep -Fq "(ACCESS_DENIED)" && echo OK

$CLICKHOUSE_CLIENT -q "GRANT UNDROP TABLE ON shard_0.t_05812 TO $user"
$CLICKHOUSE_CLIENT --user "$user" --distributed_ddl_task_timeout 0 -q "UNDROP TABLE t_05812 ON CLUSTER test_cluster_two_shards_different_databases" 2>&1 | grep -Fq "(ACCESS_DENIED)" && echo OK
$CLICKHOUSE_CLIENT -q "GRANT UNDROP TABLE ON shard_1.t_05812 TO $user"
out=$($CLICKHOUSE_CLIENT --user "$user" --distributed_ddl_task_timeout 0 -q "UNDROP TABLE t_05812 ON CLUSTER test_cluster_two_shards_different_databases" 2>&1)
[ -z "$out" ] && echo OK

$CLICKHOUSE_CLIENT -q "DROP DATABASE ${CLICKHOUSE_DATABASE}_rep"
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${CLICKHOUSE_DATABASE}_other"
$CLICKHOUSE_CLIENT -q "DROP USER $user"
