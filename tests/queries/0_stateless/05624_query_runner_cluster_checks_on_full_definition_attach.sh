#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The cluster mode of a `QueryRunner` table requires the `REMOTE` grant and rejects `SQL SECURITY` for a
# cluster without `secret`. A full-definition ATTACH is checked like CREATE.

user="user_${CLICKHOUSE_DATABASE}"
ENGINE="QueryRunner SETTINGS cluster = 'test_shard_localhost', mode = 'synchronous'"
db_engine=$(${CLICKHOUSE_CLIENT} -q "SELECT engine FROM system.databases WHERE name = currentDatabase()")
uuid_clause() { [ "${db_engine}" = "Ordinary" ] || echo "UUID '$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4()")'"; }

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user} IDENTIFIED WITH plaintext_password BY 'password'"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE TABLE, DROP TABLE ON ${CLICKHOUSE_DATABASE}.* TO ${user}"
${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON QueryRunner TO ${user}"
${CLICKHOUSE_CLIENT} -q "GRANT CLUSTER ON *.* TO ${user}"
${CLICKHOUSE_CLIENT} -q "GRANT SHOW TABLES, SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.* TO ${user}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE runner_src (query String) ENGINE = ${ENGINE}"
REPEATED="QueryRunner SETTINGS cluster = '', cluster = 'test_shard_localhost', mode = 'synchronous'"

CLIENT_AS_USER="${CLICKHOUSE_CLIENT} --user ${user} --password password"

echo "without the REMOTE grant"
${CLIENT_AS_USER} -q "CREATE TABLE runner_c (query String) ENGINE = ${ENGINE}; -- { serverError ACCESS_DENIED }"
${CLIENT_AS_USER} -q "ATTACH TABLE runner_a $(uuid_clause) (query String) ENGINE = ${ENGINE}; -- { serverError ACCESS_DENIED }"
${CLIENT_AS_USER} -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.runner_c ON CLUSTER test_shard_localhost (query String) ENGINE = ${ENGINE}; -- { serverError ACCESS_DENIED }"
${CLIENT_AS_USER} -q "ATTACH TABLE ${CLICKHOUSE_DATABASE}.runner_a $(uuid_clause) ON CLUSTER test_shard_localhost (query String) ENGINE = ${ENGINE}; -- { serverError ACCESS_DENIED }"
${CLIENT_AS_USER} -q "CREATE TABLE runner_r (query String) ENGINE = ${REPEATED}; -- { serverError ACCESS_DENIED }"
${CLIENT_AS_USER} -q "CREATE TABLE runner_local (query String) ENGINE = QueryRunner SETTINGS cluster = 'test_shard_localhost', cluster = '', mode = 'synchronous'"
${CLIENT_AS_USER} -q "DROP TABLE runner_local"
${CLIENT_AS_USER} -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.runner_r ON CLUSTER test_shard_localhost (query String) ENGINE = ${REPEATED}; -- { serverError ACCESS_DENIED }"
${CLIENT_AS_USER} -q "CREATE TABLE runner_copy AS runner_src; -- { serverError ACCESS_DENIED }"
${CLIENT_AS_USER} -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.runner_copy ON CLUSTER test_shard_localhost AS ${CLICKHOUSE_DATABASE}.runner_src; -- { serverError ACCESS_DENIED }"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name != 'runner_src'"

echo "with the REMOTE grant"
${CLICKHOUSE_CLIENT} -q "GRANT READ, WRITE ON REMOTE TO ${user}"
${CLIENT_AS_USER} -q "CREATE TABLE runner_c (query String) ENGINE = ${ENGINE}"
${CLIENT_AS_USER} --send_logs_level=error -q "ATTACH TABLE runner_a $(uuid_clause) (query String) ENGINE = ${ENGINE}"
${CLIENT_AS_USER} -q "CREATE TABLE runner_r (query String) ENGINE = ${REPEATED}"
${CLIENT_AS_USER} -q "CREATE TABLE runner_copy AS runner_src"
${CLICKHOUSE_CLIENT} -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name != 'runner_src' ORDER BY name"
${CLIENT_AS_USER} -q "DROP TABLE runner_c"
${CLIENT_AS_USER} -q "DROP TABLE runner_a"
${CLIENT_AS_USER} -q "DROP TABLE runner_r"
${CLIENT_AS_USER} -q "DROP TABLE runner_copy"

echo "SQL SECURITY with a cluster without secret"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE runner_c (query String) ENGINE = ${ENGINE} SQL SECURITY NONE; -- { serverError BAD_ARGUMENTS }"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE runner_a $(uuid_clause) (query String) ENGINE = ${ENGINE} SQL SECURITY NONE; -- { serverError BAD_ARGUMENTS }"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name != 'runner_src'"

${CLICKHOUSE_CLIENT} -q "DROP TABLE runner_src"
${CLICKHOUSE_CLIENT} -q "DROP USER ${user}"
