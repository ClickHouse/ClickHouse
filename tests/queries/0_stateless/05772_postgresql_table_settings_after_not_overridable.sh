#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: depends on libpqxx (the PostgreSQL table engine), which is not built in fast test.
# no-replicated-database: a named collection created by a query exists only on its server, so the other replicas of the
# database cannot create the table.
#
# The `SETTINGS` clause of a `PostgreSQL` table overrides a key of its named collection. If the collection marks the key
# `NOT OVERRIDABLE` later, the stored definition still attaches and keeps its value, while a definition that a user gives
# with the same override is rejected. The engine does not connect at `CREATE` or `ATTACH`, so the host does not need to be
# reachable.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

NC="nc_later_locked_table_pool_${CLICKHOUSE_DATABASE}"
NC_KEYS="host = '127.0.0.1', port = 5432, database = 'db', table = 't', user = 'user', password = 'password'"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.t_pg"
    ${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION IF EXISTS ${NC}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "
    CREATE NAMED COLLECTION ${NC} AS ${NC_KEYS}, postgresql_connection_pool_size = 7;
    CREATE TABLE t_pg (x UInt32) ENGINE = PostgreSQL(${NC}) SETTINGS postgresql_connection_pool_size = 3;
    ALTER NAMED COLLECTION ${NC} SET postgresql_connection_pool_size = 7 NOT OVERRIDABLE;
    DETACH TABLE t_pg;"

echo "--- the stored definition attaches and keeps its value"
${CLICKHOUSE_CLIENT} --send_logs_level trace --query "ATTACH TABLE t_pg" 2>&1 \
    | grep -o -m1 'PostgreSQL connection pool size: [0-9]*'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 't_pg'"

echo "--- a definition that a user gives with the same override is rejected"
${CLICKHOUSE_CLIENT} \
    --query "CREATE TABLE t_pg_new (x UInt32) ENGINE = PostgreSQL(${NC}) SETTINGS postgresql_connection_pool_size = 3" 2>&1 \
    | grep -o -m1 "Override not allowed for 'postgresql_connection_pool_size'"
