#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the `PostgreSQL` database engine

# `SYSTEM START MERGES` on an `Alias` resolves its target to release a `SYSTEM STOP MERGES` taken on it. The
# target is looked up only when the user may run the statement on it as well: the lookup may reach an external
# database, here an unreachable `PostgreSQL` one, and its error would leak to a user granted only the alias.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TARGET_DB="${CLICKHOUSE_DATABASE}_target"
ALIAS_USER="user_alias_only_${CLICKHOUSE_DATABASE}"
FULL_USER="user_alias_full_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${ALIAS_USER}, ${FULL_USER}"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${TARGET_DB}"

# An `Alias` needs an existing target, so the database of the target is replaced afterwards.
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${TARGET_DB}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${TARGET_DB}.t (x UInt8) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE a ENGINE = Alias('${TARGET_DB}', 't')"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${TARGET_DB} SYNC"
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${TARGET_DB} ENGINE = PostgreSQL('127.0.0.1:1', 'db', 'user', 'password')"

${CLICKHOUSE_CLIENT} -q "CREATE USER ${ALIAS_USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${FULL_USER} IDENTIFIED WITH no_password"
${CLICKHOUSE_CLIENT} -q "GRANT SYSTEM MERGES ON ${CLICKHOUSE_DATABASE}.a TO ${ALIAS_USER}, ${FULL_USER}"
${CLICKHOUSE_CLIENT} -q "GRANT SYSTEM MERGES ON ${TARGET_DB}.t TO ${FULL_USER}"

echo "alias only"
${CLICKHOUSE_CLIENT} --user "${ALIAS_USER}" -q "SYSTEM START MERGES ${CLICKHOUSE_DATABASE}.a" && echo "OK"
# The control: a user who may run the statement on the target makes the lookup reach the external database.
echo "alias and target"
${CLICKHOUSE_CLIENT} --user "${FULL_USER}" -q "SYSTEM START MERGES ${CLICKHOUSE_DATABASE}.a" 2>&1 | grep -o -m1 "POSTGRESQL_CONNECTION_FAILURE"

${CLICKHOUSE_CLIENT} -q "DROP USER ${ALIAS_USER}, ${FULL_USER}"
${CLICKHOUSE_CLIENT} -q "DROP TABLE a"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${TARGET_DB}"
