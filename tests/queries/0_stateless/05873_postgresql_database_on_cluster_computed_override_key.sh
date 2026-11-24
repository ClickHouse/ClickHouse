#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: depends on libpqxx (the PostgreSQL database engine), which is not built in fast test.
# no-parallel: the databases created here point at an unreachable host, and a concurrent statement over all databases,
#   such as `SYSTEM STOP MERGES`, would try to connect there and log connection errors.
#
# The key of an override of the named collection of a `PostgreSQL` database is checked before `ON CLUSTER` is dispatched, and
# the workers, which run without the user, get the key that was checked, not the expression, which they could evaluate to
# another key, such as `currentUser` here. The engine does not connect at `CREATE`, so the host does not need to be reachable.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_pg"
NC="nc_key_${CLICKHOUSE_DATABASE}"
USER="u_pg_key_${CLICKHOUSE_DATABASE}"

function cleanup()
{
    for suffix in create attach denied; do
        ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB}_${suffix}"
    done
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${USER}"
    ${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION IF EXISTS ${NC}"
}
trap cleanup EXIT

function engine_arguments_of()
{
    ${CLICKHOUSE_CLIENT} --query "SELECT extract(engine_full, '\\\\((.*)\\\\)') FROM system.databases WHERE name = '${DB}_$1' FORMAT TSVRaw"
}

${CLICKHOUSE_CLIENT} --query "
    CREATE NAMED COLLECTION ${NC} AS host = '127.0.0.1', port = 5432, database = 'db', user = 'user', password = 'password';
    CREATE USER ${USER};
    GRANT CREATE DATABASE ON ${DB}_create.* TO ${USER};
    GRANT CREATE DATABASE ON ${DB}_attach.* TO ${USER};
    GRANT CREATE DATABASE ON ${DB}_denied.* TO ${USER};
    GRANT POSTGRES, CLUSTER ON *.* TO ${USER};
    GRANT NAMED COLLECTION ON ${NC} TO ${USER};"

# `schema` for the user, who may add it, and `host` for anyone else, which needs `SHOW NAMED COLLECTIONS SECRETS`.
KEY="if(currentUser() = '${USER}', 'schema', 'host')"

echo "--- the workers get the key that was checked"
${CLICKHOUSE_CLIENT} --user "${USER}" --query "
    CREATE DATABASE ${DB}_create ON CLUSTER test_shard_localhost ENGINE = PostgreSQL(${NC}, ${KEY} = '127.0.0.2')" > /dev/null
engine_arguments_of create | sed "s/${NC}/nc/"
${CLICKHOUSE_CLIENT} --user "${USER}" --query "
    ATTACH DATABASE ${DB}_attach ON CLUSTER test_shard_localhost ENGINE = PostgreSQL(${NC}, ${KEY} = '127.0.0.2')" > /dev/null
engine_arguments_of attach | sed "s/${NC}/nc/"

echo "--- a key that is computed to a key the user may not override is rejected"
${CLICKHOUSE_CLIENT} --user "${USER}" --query "
    CREATE DATABASE ${DB}_denied ON CLUSTER test_shard_localhost
    ENGINE = PostgreSQL(${NC}, if(currentUser() = '${USER}', 'host', 'schema') = '127.0.0.2')" 2>&1 | grep -o -m1 'ACCESS_DENIED'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.databases WHERE name = '${DB}_denied'"
