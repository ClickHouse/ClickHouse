#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: depends on libpqxx (the PostgreSQL database engine), which is not built in fast test.
#
# The checks of the `SETTINGS` clause of a `PostgreSQL` database: a stated value equal to the current one stays in the
# definition, also over a named collection, an override of a key of a named collection needs `SHOW NAMED COLLECTIONS SECRETS`,
# and the user's constraints and that privilege are checked before `ON CLUSTER` is dispatched, whose workers run without the
# user. The engine does not connect at `CREATE`, so the host does not need to be reachable.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_pg"
NC="nc_pool_${CLICKHOUSE_DATABASE}"
NC_KEYS="host = '127.0.0.1', port = 5432, database = 'db', user = 'user', password = 'password'"
USER="u_pg_settings_${CLICKHOUSE_DATABASE}"
PROFILE="p_pg_settings_${CLICKHOUSE_DATABASE}"

function cleanup()
{
    for suffix in same collection restricted restricted_attach cluster cluster_attach not_granted; do
        ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB}_${suffix}"
    done
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${USER}"
    ${CLICKHOUSE_CLIENT} --query "DROP SETTINGS PROFILE IF EXISTS ${PROFILE}"
    ${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION IF EXISTS ${NC}"
}
trap cleanup EXIT

function settings_of()
{
    ${CLICKHOUSE_CLIENT} --query "SELECT extract(engine_full, 'SETTINGS .*') FROM system.databases WHERE name = '${DB}_$1'"
}

function exists()
{
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.databases WHERE name = '${DB}_$1'"
}

${CLICKHOUSE_CLIENT} --query "CREATE NAMED COLLECTION ${NC} AS ${NC_KEYS}, postgresql_connection_pool_size = 7"

echo "--- a stated value equal to the value of the session stays in the definition"
${CLICKHOUSE_CLIENT} --postgresql_connection_pool_size 16 --query "
    CREATE DATABASE ${DB}_same ENGINE = PostgreSQL('127.0.0.1:5432', 'db', 'user', 'password')
    SETTINGS postgresql_connection_pool_size = 16"
settings_of same
${CLICKHOUSE_CLIENT} --query "DETACH DATABASE ${DB}_same"
${CLICKHOUSE_CLIENT} --query "ATTACH DATABASE ${DB}_same"
settings_of same

echo "--- and takes precedence over the named collection"
${CLICKHOUSE_CLIENT} --postgresql_connection_pool_size 16 --send_logs_level trace --query "
    CREATE DATABASE ${DB}_collection ENGINE = PostgreSQL(${NC}) SETTINGS postgresql_connection_pool_size = 16" 2>&1 \
    | grep -o -m1 'PostgreSQL connection pool size: [0-9]*'
settings_of collection

${CLICKHOUSE_CLIENT} --query "
    CREATE SETTINGS PROFILE ${PROFILE} SETTINGS postgresql_connection_pool_size = 3 MAX 5;
    CREATE USER ${USER} SETTINGS PROFILE '${PROFILE}';
    GRANT CREATE DATABASE ON ${DB}_restricted.* TO ${USER};
    GRANT CREATE DATABASE ON ${DB}_restricted_attach.* TO ${USER};
    GRANT CREATE DATABASE ON ${DB}_cluster.* TO ${USER};
    GRANT CREATE DATABASE ON ${DB}_cluster_attach.* TO ${USER};
    GRANT POSTGRES, CLUSTER ON *.* TO ${USER};
    GRANT NAMED COLLECTION ON ${NC} TO ${USER};"

echo "--- an override of a key of the named collection needs SHOW NAMED COLLECTIONS SECRETS"
${CLICKHOUSE_CLIENT} --user "${USER}" --query "
    CREATE DATABASE ${DB}_restricted ENGINE = PostgreSQL(${NC}) SETTINGS postgresql_connection_pool_size = 4" 2>&1 \
    | grep -o -m1 'ACCESS_DENIED'
${CLICKHOUSE_CLIENT} --user "${USER}" --query "
    ATTACH DATABASE ${DB}_restricted_attach ENGINE = PostgreSQL(${NC}) SETTINGS postgresql_connection_pool_size = 4" 2>&1 \
    | grep -o -m1 'ACCESS_DENIED'
exists restricted
exists restricted_attach

echo "--- the checks run before ON CLUSTER is dispatched"
${CLICKHOUSE_CLIENT} --user "${USER}" --query "
    CREATE DATABASE ${DB}_cluster ON CLUSTER test_shard_localhost ENGINE = PostgreSQL('127.0.0.1:5432', 'db', 'user', 'password')
    SETTINGS postgresql_connection_pool_size = 100" 2>&1 | grep -o -m1 'SETTING_CONSTRAINT_VIOLATION'
${CLICKHOUSE_CLIENT} --user "${USER}" --query "
    ATTACH DATABASE ${DB}_cluster_attach ON CLUSTER test_shard_localhost ENGINE = PostgreSQL('127.0.0.1:5432', 'db', 'user', 'password')
    SETTINGS postgresql_connection_pool_size = 100" 2>&1 | grep -o -m1 'SETTING_CONSTRAINT_VIOLATION'
${CLICKHOUSE_CLIENT} --user "${USER}" --query "
    CREATE DATABASE ${DB}_cluster ON CLUSTER test_shard_localhost ENGINE = PostgreSQL(${NC})
    SETTINGS postgresql_connection_pool_size = 4" 2>&1 | grep -o -m1 'ACCESS_DENIED'
exists cluster
exists cluster_attach

echo "--- a user who may not create the database gets that error first"
${CLICKHOUSE_CLIENT} --user "${USER}" --query "
    CREATE DATABASE ${DB}_not_granted ON CLUSTER test_shard_localhost ENGINE = PostgreSQL(${NC})
    SETTINGS postgresql_connection_pool_size = 100" 2>&1 | grep -o -m1 'necessary to have the grant CREATE DATABASE'

echo "--- the override is allowed with SHOW NAMED COLLECTIONS SECRETS"
${CLICKHOUSE_CLIENT} --query "GRANT SHOW NAMED COLLECTIONS SECRETS ON ${NC} TO ${USER}"
${CLICKHOUSE_CLIENT} --user "${USER}" --query "
    CREATE DATABASE ${DB}_restricted ENGINE = PostgreSQL(${NC}) SETTINGS postgresql_connection_pool_size = 4"
${CLICKHOUSE_CLIENT} --user "${USER}" --query "
    ATTACH DATABASE ${DB}_restricted_attach ENGINE = PostgreSQL(${NC}) SETTINGS postgresql_connection_pool_size = 4"
settings_of restricted
settings_of restricted_attach
