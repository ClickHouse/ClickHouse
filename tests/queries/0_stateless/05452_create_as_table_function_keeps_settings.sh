#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: depends on libpqxx and mysqlxx (the `postgresql` and `mysql` table functions), not built in fast test.
#
# `CREATE TABLE ... AS postgresql(..., SETTINGS ...)` and `AS mysql(..., SETTINGS ...)` store the call as the definition
# of the table, and the stored call keeps its SETTINGS argument, so the table uses the same settings when it is
# attached again, e.g. on server startup (https://github.com/ClickHouse/ClickHouse/issues/127823). Nothing listens at
# the addresses: neither table connects when its columns are given.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `Replicated` database prints the status of each DDL query otherwise.
DDL_CLIENT="${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none"

function definition_of()
{
    ${CLICKHOUSE_CLIENT} --query "SELECT extract(create_table_query, ' AS .*') FROM system.tables WHERE database = currentDatabase() AND name = '$1'"
}

echo "--- postgresql"
${DDL_CLIENT} --query "
    CREATE TABLE t_pg (x Int32) AS postgresql('127.0.0.1:5432', 'db', 'tbl', 'user', 'password', SETTINGS postgresql_connection_pool_size = 3)"
definition_of t_pg
${DDL_CLIENT} --query "DETACH TABLE t_pg PERMANENTLY"
${DDL_CLIENT} --send_logs_level trace --query "ATTACH TABLE t_pg" 2>&1 | grep -o -m1 'PostgreSQL connection pool size: [0-9]*'

echo "--- mysql"
${DDL_CLIENT} --query "
    CREATE TABLE t_my (x Int32) AS mysql('127.0.0.1:3306', 'db', 'tbl', 'user', 'password', SETTINGS connect_timeout = 1)"
definition_of t_my
${DDL_CLIENT} --query "DETACH TABLE t_my PERMANENTLY"
${DDL_CLIENT} --query "ATTACH TABLE t_my"
definition_of t_my

${DDL_CLIENT} --query "DROP TABLE t_pg; DROP TABLE t_my"
