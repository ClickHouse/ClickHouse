#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: depends on libpqxx (the PostgreSQL database engine), which is not built in fast test.
# no-parallel: the databases created here point at an unreachable host, and a concurrent statement over all databases,
#   such as `SYSTEM STOP MERGES`, would try to connect there and log connection errors.
#
# A name in the `SETTINGS` clause of a `PostgreSQL` database that is neither a setting of the engine nor a query setting is
# rejected, also as `name = DEFAULT` or `param_x = ...`, which the engine does not apply, and also before `ON CLUSTER` is
# dispatched. The engine does not connect at `CREATE`, so the host does not need to be reachable.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_pg"
ENGINE="ENGINE = PostgreSQL('127.0.0.1:5432', 'db', 'user', 'password')"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB}"
}
trap cleanup EXIT

function create()
{
    # A rejection by a worker after `ON CLUSTER` is dispatched comes back as `There was an error on [host:port]: ...`.
    ${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${DB} $1 ${ENGINE} SETTINGS $2" 2>&1 \
        | grep -o -m1 -E 'There was an error on|UNKNOWN_SETTING' || echo 'created'
    ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB}"
}

echo "--- an unknown name"
create "" "postgresql_connection_pool_siez = 3"
create "" "postgresql_connection_pool_siez = DEFAULT"
create "" "param_postgresql_connection_pool_size = 3"
create "ON CLUSTER test_shard_localhost" "postgresql_connection_pool_siez = 3"
create "ON CLUSTER test_shard_localhost" "postgresql_connection_pool_siez = DEFAULT"
create "ON CLUSTER test_shard_localhost" "param_postgresql_connection_pool_size = 3"

echo "--- a setting of the engine or a query setting"
create "" "postgresql_connection_pool_size = DEFAULT"
create "" "max_threads = DEFAULT"
create "ON CLUSTER test_shard_localhost" "postgresql_connection_pool_size = DEFAULT"
