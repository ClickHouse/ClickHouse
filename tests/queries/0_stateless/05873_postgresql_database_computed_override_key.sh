#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: depends on libpqxx (the PostgreSQL database engine), which is not built in fast test.
# no-parallel: the databases created here point at an unreachable host, and a concurrent statement over all databases,
#   such as `SYSTEM STOP MERGES`, would try to connect there and log connection errors.
#
# A computed key of an override of the named collection of a `PostgreSQL` database is stored and dispatched as the key that it
# was checked as, not as the expression, which could become another key with another context: on the workers of `ON CLUSTER`,
# which run without the user, or on a short `ATTACH DATABASE` by another user, such as with `currentUser` here. The engine does
# not connect at `CREATE`, so the host does not need to be reachable.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_pg"
NC="nc_key_${CLICKHOUSE_DATABASE}"
USER="u_pg_key_${CLICKHOUSE_DATABASE}"

function cleanup()
{
    for suffix in local local_attach cluster cluster_attach denied; do
        ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB}_${suffix}"
    done
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${USER}"
    ${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION IF EXISTS ${NC}"
}
trap cleanup EXIT

function engine_arguments_of()
{
    ${CLICKHOUSE_CLIENT} --query "SELECT extract(engine_full, '\\\\((.*)\\\\)') FROM system.databases WHERE name = '${DB}_$1' FORMAT TSVRaw" \
        | sed "s/${NC}/nc/"
}

${CLICKHOUSE_CLIENT} --query "
    CREATE NAMED COLLECTION ${NC} AS host = '127.0.0.1', port = 5432, database = 'db', user = 'user', password = 'password';
    CREATE USER ${USER};
    GRANT CREATE DATABASE ON ${DB}_local.* TO ${USER};
    GRANT CREATE DATABASE ON ${DB}_local_attach.* TO ${USER};
    GRANT CREATE DATABASE ON ${DB}_cluster.* TO ${USER};
    GRANT CREATE DATABASE ON ${DB}_cluster_attach.* TO ${USER};
    GRANT CREATE DATABASE ON ${DB}_denied.* TO ${USER};
    GRANT POSTGRES, CLUSTER ON *.* TO ${USER};
    GRANT NAMED COLLECTION ON ${NC} TO ${USER};"

# `schema` for the user, who may add it, and `host` for anyone else, which needs `SHOW NAMED COLLECTIONS SECRETS`.
KEY="if(currentUser() = '${USER}', 'schema', 'host')"

echo "--- a definition stores the key that was checked, also after a short ATTACH by another user"
${CLICKHOUSE_CLIENT} --user "${USER}" --query "CREATE DATABASE ${DB}_local ENGINE = PostgreSQL(${NC}, ${KEY} = '127.0.0.2')"
engine_arguments_of local
${CLICKHOUSE_CLIENT} --user "${USER}" --query "ATTACH DATABASE ${DB}_local_attach ENGINE = PostgreSQL(${NC}, ${KEY} = '127.0.0.2')"
engine_arguments_of local_attach
${CLICKHOUSE_CLIENT} --query "DETACH DATABASE ${DB}_local"
${CLICKHOUSE_CLIENT} --query "ATTACH DATABASE ${DB}_local"
engine_arguments_of local

echo "--- the workers of ON CLUSTER get the key that was checked"
${CLICKHOUSE_CLIENT} --user "${USER}" --query "
    CREATE DATABASE ${DB}_cluster ON CLUSTER test_shard_localhost ENGINE = PostgreSQL(${NC}, ${KEY} = '127.0.0.2')" > /dev/null
engine_arguments_of cluster
${CLICKHOUSE_CLIENT} --user "${USER}" --query "
    ATTACH DATABASE ${DB}_cluster_attach ON CLUSTER test_shard_localhost ENGINE = PostgreSQL(${NC}, ${KEY} = '127.0.0.2')" > /dev/null
engine_arguments_of cluster_attach

echo "--- a key that is computed to a key the user may not override is rejected"
${CLICKHOUSE_CLIENT} --user "${USER}" --query "
    CREATE DATABASE ${DB}_denied ON CLUSTER test_shard_localhost
    ENGINE = PostgreSQL(${NC}, if(currentUser() = '${USER}', 'host', 'schema') = '127.0.0.2')" 2>&1 | grep -o -m1 'ACCESS_DENIED'
${CLICKHOUSE_CLIENT} --user "${USER}" --query "
    CREATE DATABASE ${DB}_denied ENGINE = PostgreSQL(${NC}, if(currentUser() = '${USER}', 'host', 'schema') = '127.0.0.2')" 2>&1 \
    | grep -o -m1 'ACCESS_DENIED'
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.databases WHERE name = '${DB}_denied'"
