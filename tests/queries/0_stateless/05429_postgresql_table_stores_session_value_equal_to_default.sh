#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: depends on libpqxx (the PostgreSQL table engine), which is not built in fast test.
# no-replicated-database: a replica drops a setting of the replicated `CREATE` that equals its own value (the settings
# constraints skip unchanged values), so there the session does not change it and nothing is stored.
#
# A `PostgreSQL` table stores a pool setting that the session sets even if the value equals the default of the engine:
# without it, the table would take the server's value when attached again, e.g. on server startup.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With `SET`: the client does not send a setting of its command line that equals the default.
${CLICKHOUSE_CLIENT} --query "
    SET postgresql_connection_pool_size = 16;
    CREATE TABLE t_session_default (x Int32) ENGINE = PostgreSQL('127.0.0.1:5432', 'db', 'tbl', 'user', 'password');"
${CLICKHOUSE_CLIENT} --query "SELECT extract(create_table_query, 'SETTINGS .*') FROM system.tables WHERE database = currentDatabase() AND name = 't_session_default'"
${CLICKHOUSE_CLIENT} --query "DROP TABLE t_session_default"
