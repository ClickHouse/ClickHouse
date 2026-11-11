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

CLIENT_AS_USER="${CLICKHOUSE_CLIENT} --user ${user} --password password"

echo "without the REMOTE grant"
${CLIENT_AS_USER} -q "CREATE TABLE runner_c (query String) ENGINE = ${ENGINE}; -- { serverError ACCESS_DENIED }"
${CLIENT_AS_USER} -q "ATTACH TABLE runner_a $(uuid_clause) (query String) ENGINE = ${ENGINE}; -- { serverError ACCESS_DENIED }"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = currentDatabase()"

echo "with the REMOTE grant"
${CLICKHOUSE_CLIENT} -q "GRANT READ, WRITE ON REMOTE TO ${user}"
${CLIENT_AS_USER} -q "CREATE TABLE runner_c (query String) ENGINE = ${ENGINE}"
${CLIENT_AS_USER} --send_logs_level=error -q "ATTACH TABLE runner_a $(uuid_clause) (query String) ENGINE = ${ENGINE}"
${CLICKHOUSE_CLIENT} -q "SELECT name FROM system.tables WHERE database = currentDatabase() ORDER BY name"
${CLIENT_AS_USER} -q "DROP TABLE runner_c"
${CLIENT_AS_USER} -q "DROP TABLE runner_a"

echo "SQL SECURITY with a cluster without secret"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE runner_c (query String) ENGINE = ${ENGINE} SQL SECURITY NONE; -- { serverError BAD_ARGUMENTS }"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE runner_a $(uuid_clause) (query String) ENGINE = ${ENGINE} SQL SECURITY NONE; -- { serverError BAD_ARGUMENTS }"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = currentDatabase()"

${CLICKHOUSE_CLIENT} -q "DROP USER ${user}"
