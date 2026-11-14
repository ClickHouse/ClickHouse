#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: depends on libpqxx (the PostgreSQL database engine), which is not built in fast test.
# no-parallel: the databases created here point at an unreachable host, and a concurrent statement over all databases,
#   such as `SYSTEM STOP MERGES`, would try to connect there and log connection errors.
#
# A `PostgreSQL` database with a named collection: its `SETTINGS` clause takes precedence over the collection, unless the
# collection marks the setting `NOT OVERRIDABLE`, and a value that the collection sets is not taken from the session, so
# nothing is stored for it (see 05565 for a database without a collection). The engine does not connect at `CREATE`, so the
# host does not need to be reachable.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_pg"
NC_WITH="nc_with_pool_${CLICKHOUSE_DATABASE}"
NC_LOCKED="nc_locked_pool_${CLICKHOUSE_DATABASE}"
NC_WITHOUT="nc_without_pool_${CLICKHOUSE_DATABASE}"
NC_KEYS="host = '127.0.0.1', port = 5432, database = 'db', user = 'user', password = 'password'"

function cleanup()
{
    for suffix in stated session locked without; do
        ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB}_${suffix}"
    done
    for nc in ${NC_WITH} ${NC_LOCKED} ${NC_WITHOUT}; do
        ${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION IF EXISTS ${nc}"
    done
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

${CLICKHOUSE_CLIENT} --query "
    CREATE NAMED COLLECTION ${NC_WITH} AS ${NC_KEYS}, postgresql_connection_pool_size = 7;
    CREATE NAMED COLLECTION ${NC_LOCKED} AS ${NC_KEYS}, postgresql_connection_pool_size = 7 NOT OVERRIDABLE;
    CREATE NAMED COLLECTION ${NC_WITHOUT} AS ${NC_KEYS};"

echo "--- a stated value takes precedence over the named collection"
${CLICKHOUSE_CLIENT} --send_logs_level trace \
    --query "CREATE DATABASE ${DB}_stated ENGINE = PostgreSQL(${NC_WITH}) SETTINGS postgresql_connection_pool_size = 3" 2>&1 | pool_size
settings_of stated

echo "--- a value set by the named collection is not taken from the session, so nothing is stored"
${CLICKHOUSE_CLIENT} --send_logs_level trace --postgresql_connection_pool_size 5 \
    --query "CREATE DATABASE ${DB}_session ENGINE = PostgreSQL(${NC_WITH})" 2>&1 | pool_size
settings_of session

echo "--- a stated value is rejected when the named collection does not allow to override it"
${CLICKHOUSE_CLIENT} \
    --query "CREATE DATABASE ${DB}_locked ENGINE = PostgreSQL(${NC_LOCKED}) SETTINGS postgresql_connection_pool_size = 3" 2>&1 \
    | grep -o -m1 "Override not allowed for 'postgresql_connection_pool_size'"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.databases WHERE name = '${DB}_locked'"

echo "--- a value the named collection does not set is taken from the session and stored"
${CLICKHOUSE_CLIENT} --postgresql_connection_pool_size 5 --query "CREATE DATABASE ${DB}_without ENGINE = PostgreSQL(${NC_WITHOUT})"
settings_of without
