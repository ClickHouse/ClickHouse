#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: depends on libpqxx (the PostgreSQL database engine), which is not built in fast test.
# no-parallel: the database created here points at an unreachable host, and a concurrent statement over all databases,
#   such as `SYSTEM STOP MERGES`, would try to connect there and log connection errors.
#
# The `SETTINGS` clause of a `PostgreSQL` database overrides a key of its named collection. If the collection marks the key
# `NOT OVERRIDABLE` later, the stored definition still loads, as at the startup of the server, and keeps its value, while a
# definition that a user gives with the same override is rejected. The engine does not connect at `CREATE` or `ATTACH`, so
# the host does not need to be reachable.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_pg"
NC="nc_later_locked_pool_${CLICKHOUSE_DATABASE}"
NC_KEYS="host = '127.0.0.1', port = 5432, database = 'db', user = 'user', password = 'password'"

function cleanup()
{
    # A stored definition that failed to load stays detached: unlock the key so that it attaches and is dropped, rather than
    # left behind over a dropped named collection, which would stop the next start of the server.
    ${CLICKHOUSE_CLIENT} --query "ALTER NAMED COLLECTION ${NC} SET postgresql_connection_pool_size = 7 OVERRIDABLE" 2>/dev/null
    ${CLICKHOUSE_CLIENT} --query "ATTACH DATABASE ${DB}" 2>/dev/null
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB}"
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB}_attached"
    ${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION IF EXISTS ${NC}"
}
trap cleanup EXIT

function pool_size()
{
    grep -o -m1 'PostgreSQL connection pool size: [0-9]*'
}

${CLICKHOUSE_CLIENT} --query "
    CREATE NAMED COLLECTION ${NC} AS ${NC_KEYS}, postgresql_connection_pool_size = 7;
    CREATE DATABASE ${DB} ENGINE = PostgreSQL(${NC}) SETTINGS postgresql_connection_pool_size = 3;
    ALTER NAMED COLLECTION ${NC} SET postgresql_connection_pool_size = 7 NOT OVERRIDABLE;
    DETACH DATABASE ${DB};"

echo "--- the stored definition loads and keeps its value"
${CLICKHOUSE_CLIENT} --send_logs_level trace --query "ATTACH DATABASE ${DB}" 2>&1 | pool_size
${CLICKHOUSE_CLIENT} --query "SELECT extract(engine_full, 'SETTINGS .*') FROM system.databases WHERE name = '${DB}'"

echo "--- a definition that a user gives with the same override is rejected"
${CLICKHOUSE_CLIENT} \
    --query "ATTACH DATABASE ${DB}_attached ENGINE = PostgreSQL(${NC}) SETTINGS postgresql_connection_pool_size = 3" 2>&1 \
    | grep -o -m1 "Override not allowed for 'postgresql_connection_pool_size'"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.databases WHERE name = '${DB}_attached'"
