#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: depends on libpqxx (the PostgreSQL table engine), which is not built in fast test.
# no-replicated-database: named collections are server-global, not database-scoped.
#
# A `PostgreSQL` table takes the connection pool settings that neither its definition nor its named collection states
# from the session that creates it, and stores those (05346). A value that the named collection sets is not taken from
# the session, so nothing is stored for it. The engine does not connect at `CREATE` when the columns are given, so the
# host does not need to be reachable.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

NC_WITH="nc_with_pool_${CLICKHOUSE_DATABASE}"
NC_WITHOUT="nc_without_pool_${CLICKHOUSE_DATABASE}"
NC_KEYS="host = '127.0.0.1', port = 5432, database = 'db', table = 'tbl', user = 'user', password = 'password'"

function settings_of()
{
    ${CLICKHOUSE_CLIENT} --query "SELECT extract(create_table_query, 'SETTINGS .*') FROM system.tables WHERE database = currentDatabase() AND name = '$1'"
}

${CLICKHOUSE_CLIENT} --query "
    CREATE NAMED COLLECTION ${NC_WITH} AS ${NC_KEYS}, postgresql_connection_pool_size = 5;
    CREATE NAMED COLLECTION ${NC_WITHOUT} AS ${NC_KEYS};"

echo "--- a value set by the named collection is not taken from the session, so nothing is stored"
${CLICKHOUSE_CLIENT} --send_logs_level trace --postgresql_connection_pool_size 3 \
    --query "CREATE TABLE t_nc_with (x Int32) ENGINE = PostgreSQL(${NC_WITH})" 2>&1 \
    | grep -o -m1 'PostgreSQL connection pool size: [0-9]*'
settings_of t_nc_with

echo "--- a value the named collection does not set is taken from the session and stored"
${CLICKHOUSE_CLIENT} --postgresql_connection_pool_size 3 --query "CREATE TABLE t_nc_without (x Int32) ENGINE = PostgreSQL(${NC_WITHOUT})"
settings_of t_nc_without

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE t_nc_with;
    DROP TABLE t_nc_without;
    DROP NAMED COLLECTION ${NC_WITH};
    DROP NAMED COLLECTION ${NC_WITHOUT};"
