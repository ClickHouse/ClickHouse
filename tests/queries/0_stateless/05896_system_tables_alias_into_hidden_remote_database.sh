#!/usr/bin/env bash

# With `show_remote_databases_in_system_tables = 0`, the catalog tables do not list the tables of a
# remote database, so that a query can read them without contacting it. A table with the `Alias`
# engine in a local database that points into a remote one was still resolved, which contacted the
# remote server: for the access checks of `system.tables`, whatever columns the query asked for, and
# for the metadata that the other catalog tables read.
# See https://github.com/ClickHouse/ClickHouse/issues/134368

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

REMOTE_DATABASE="${CLICKHOUSE_DATABASE}_remote"

# The address `127.0.0.2` is not a local shard, so the `Remote` database asks this server over the
# network, and what it asks is seen in `system.query_log`.
${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE target (id UInt64, name String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO target VALUES (1, 'a'), (2, 'b'), (3, 'c');
    DROP DATABASE IF EXISTS ${REMOTE_DATABASE};
    CREATE DATABASE ${REMOTE_DATABASE} ENGINE = Remote('127.0.0.2:${CLICKHOUSE_PORT_TCP}', '${CLICKHOUSE_DATABASE}');
    CREATE TABLE alias_into_remote ENGINE = Alias('${REMOTE_DATABASE}', 'target');
    CREATE TABLE alias_into_local ENGINE = Alias('${CLICKHOUSE_DATABASE}', 'target');
"

# The queries that the `Remote` database has sent to this server so far. They are not initial queries
# and they name the database of this test, which no other test does.
function remote_queries()
{
    ${CLICKHOUSE_CLIENT} --query "
        SYSTEM FLUSH LOGS query_log;
        SELECT count() FROM system.query_log
        WHERE event_date >= yesterday() AND type = 'QueryFinish' AND NOT is_initial_query
            AND position(query, '${CLICKHOUSE_DATABASE}') > 0"
}

# Runs a query and prints its result and whether the remote server was asked anything meanwhile.
REMOTE_QUERIES_BEFORE=$(remote_queries)
function probe()
{
    local show_remote_databases=$1
    local query=$2
    local result
    result=$(${CLICKHOUSE_CLIENT} --show_remote_databases_in_system_tables "${show_remote_databases}" --query "${query}" | tr '\n\t' '  ')
    local remote_queries_after
    remote_queries_after=$(remote_queries)
    echo "${query} -> ${result}| contacted the remote server: $((remote_queries_after > REMOTE_QUERIES_BEFORE))"
    REMOTE_QUERIES_BEFORE=${remote_queries_after}
}

ALIASES="database = currentDatabase() AND table IN ('alias_into_remote', 'alias_into_local')"

echo "--- show_remote_databases_in_system_tables = 0: the target in the remote database is not resolved"
probe 0 "SELECT name, engine, total_rows FROM system.tables WHERE database = currentDatabase() AND name LIKE 'alias_into_%' ORDER BY name"
probe 0 "SELECT count() FROM system.tables WHERE database = currentDatabase()"
probe 0 "SELECT table, name FROM system.columns WHERE ${ALIASES} ORDER BY table, name"
probe 0 "SELECT count() FROM system.completions WHERE belongs = 'alias_into_remote' AND context = 'column'"
probe 0 "SELECT count() FROM system.data_skipping_indices WHERE ${ALIASES}"
probe 0 "SELECT count() FROM system.constraints WHERE ${ALIASES}"
probe 0 "SELECT count() FROM system.projections WHERE ${ALIASES}"

echo "--- show_remote_databases_in_system_tables = 1: it is resolved as before"
probe 1 "SELECT name, engine FROM system.tables WHERE database = currentDatabase() AND name LIKE 'alias_into_%' ORDER BY name"
probe 1 "SELECT table, name FROM system.columns WHERE ${ALIASES} ORDER BY table, name"

echo "--- an explicit SHOW COLUMNS of the alias describes its target whatever the setting is"
${CLICKHOUSE_CLIENT} --show_remote_databases_in_system_tables 0 --query "SHOW COLUMNS FROM alias_into_remote" | cut -f1,2

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE alias_into_remote;
    DROP TABLE alias_into_local;
    DROP DATABASE ${REMOTE_DATABASE};
"
