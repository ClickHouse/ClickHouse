#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: depends on libpqxx (the PostgreSQL database engine), which is not built in fast test.
#
# A `PostgreSQL` database can state the connection pool settings of its tables in a `SETTINGS` clause, and takes those it
# does not state from the session that creates it. A new database adds those that differ from the server's values to its
# `SETTINGS`, so that it keeps them when it is attached again. The test server does not change these settings, so its
# values are the defaults of the engine. The engine does not connect at `CREATE` or `ATTACH`, so the host does not need
# to be reachable.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ENGINE="PostgreSQL('127.0.0.1:5432', 'db', 'user', 'password')"
DB="${CLICKHOUSE_DATABASE}_pg"
USER="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
PROFILE="profile_${CLICKHOUSE_TEST_UNIQUE_NAME}"

function cleanup()
{
    for suffix in session server session_server stated default attach backup restored user_stated user_default; do
        ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB}_${suffix}"
    done
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${USER}"
    ${CLICKHOUSE_CLIENT} --query "DROP SETTINGS PROFILE IF EXISTS ${PROFILE}"
}
trap cleanup EXIT

function settings_of()
{
    ${CLICKHOUSE_CLIENT} --query "SELECT extract(engine_full, 'SETTINGS .*') FROM system.databases WHERE name = '${DB}_$1'"
}

function pool_size()
{
    grep -o -m1 'PostgreSQL connection pool size: [0-9]*'
}

echo "--- values taken from the session are stored"
${CLICKHOUSE_CLIENT} --postgresql_connection_pool_size 3 --postgresql_connection_pool_auto_close_connection 1 \
    --query "CREATE DATABASE ${DB}_session ENGINE = ${ENGINE}"
settings_of session

echo "--- values equal to the server's are not stored"
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${DB}_server ENGINE = ${ENGINE}"
settings_of server

echo "--- also when the session sets them"
# With `SET`: the client does not send a setting of its command line that equals the default.
${CLICKHOUSE_CLIENT} --query "
    SET postgresql_connection_pool_size = 16;
    CREATE DATABASE ${DB}_session_server ENGINE = ${ENGINE};"
settings_of session_server

echo "--- a stated value is a setting of the database and is kept as stated"
${CLICKHOUSE_CLIENT} --send_logs_level trace --postgresql_connection_pool_size 3 \
    --query "CREATE DATABASE ${DB}_stated ENGINE = ${ENGINE} SETTINGS postgresql_connection_pool_size = 5" 2>&1 | pool_size
settings_of stated

echo "--- a value reset with DEFAULT keeps the default of the engine"
${CLICKHOUSE_CLIENT} --send_logs_level trace --postgresql_connection_pool_size 3 \
    --query "CREATE DATABASE ${DB}_default ENGINE = ${ENGINE} SETTINGS postgresql_connection_pool_size = DEFAULT" 2>&1 | pool_size
settings_of default

echo "--- an ATTACH with the full definition stores them too"
${CLICKHOUSE_CLIENT} --postgresql_connection_pool_size 3 --query "ATTACH DATABASE ${DB}_attach ENGINE = ${ENGINE}"
settings_of attach

echo "--- the database keeps the stored values when attached in a session without them"
${CLICKHOUSE_CLIENT} --query "DETACH DATABASE ${DB}_stated"
${CLICKHOUSE_CLIENT} --send_logs_level trace --postgresql_connection_pool_size 7 --query "ATTACH DATABASE ${DB}_stated" 2>&1 | pool_size
settings_of stated
${CLICKHOUSE_CLIENT} --query "DETACH DATABASE ${DB}_session"
${CLICKHOUSE_CLIENT} --send_logs_level trace --query "ATTACH DATABASE ${DB}_session" 2>&1 | pool_size

echo "--- a RESTORE keeps the definition from the backup"
# `RESTORE` rejects a restored database whose definition differs from the one in the backup, so nothing is added to it.
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${DB}_backup ENGINE = ${ENGINE}"
${CLICKHOUSE_CLIENT} --query "
    BACKUP DATABASE ${DB}_backup TO Memory('${CLICKHOUSE_TEST_UNIQUE_NAME}') FORMAT Null;
    SET postgresql_connection_pool_size = 3;
    RESTORE DATABASE ${DB}_backup AS ${DB}_restored FROM Memory('${CLICKHOUSE_TEST_UNIQUE_NAME}') FORMAT Null;"
settings_of restored

echo "--- a stated value is held to the constraints of the user"
${CLICKHOUSE_CLIENT} --query "CREATE SETTINGS PROFILE ${PROFILE} SETTINGS postgresql_connection_pool_size = 3 MIN 1 MAX 5"
${CLICKHOUSE_CLIENT} --query "CREATE USER ${USER} IDENTIFIED WITH no_password SETTINGS PROFILE ${PROFILE}"
${CLICKHOUSE_CLIENT} --query "GRANT CREATE DATABASE, DROP DATABASE ON ${DB}_user_stated.*, CREATE DATABASE, DROP DATABASE ON ${DB}_user_default.* TO ${USER}"
${CLICKHOUSE_CLIENT} --query "GRANT POSTGRES ON *.* TO ${USER}"
${CLICKHOUSE_CLIENT} --user "${USER}" \
    --query "CREATE DATABASE ${DB}_user_stated ENGINE = ${ENGINE} SETTINGS postgresql_connection_pool_size = 100" 2>&1 \
    | grep -o -m1 'SETTING_CONSTRAINT_VIOLATION'
${CLICKHOUSE_CLIENT} --user "${USER}" \
    --query "CREATE DATABASE ${DB}_user_default ENGINE = ${ENGINE} SETTINGS postgresql_connection_pool_size = DEFAULT" 2>&1 \
    | grep -o -m1 'SETTING_CONSTRAINT_VIOLATION'
${CLICKHOUSE_CLIENT} --user "${USER}" \
    --query "CREATE DATABASE ${DB}_user_stated ENGINE = ${ENGINE} SETTINGS postgresql_connection_pool_size = 4"
settings_of user_stated
