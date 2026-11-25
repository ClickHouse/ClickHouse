#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: depends on libpqxx (the PostgreSQL database engine), which is not built in fast test.
# no-parallel: the databases created here point at an unreachable host, and a concurrent statement over all databases,
#   such as `SYSTEM STOP MERGES`, would try to connect there and log connection errors.
#
# `RESTORE` checks the definition of a `PostgreSQL` database from a backup as `CREATE DATABASE` checks it, also with
# `ON CLUSTER`, whose hosts create the database without the user: the user's settings constraints, the access to the named
# collection and the override of its keys. A computed key of an override in the backup is restored locally as the key that it
# is checked as, and `ON CLUSTER` rejects it, because the hosts would compute it again without the user. The engine does not
# connect at `CREATE`, so the host does not need to be reachable.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_pg"
NC="nc_restore_${CLICKHOUSE_DATABASE}"
NC_OTHER="nc_restore_other_${CLICKHOUSE_DATABASE}"
NC_KEYS="host = '127.0.0.1', port = 5432, database = 'db', user = 'user', password = 'password'"
USER="u_pg_restore_${CLICKHOUSE_DATABASE}"
PROFILE="p_pg_restore_${CLICKHOUSE_DATABASE}"
BACKUP_PREFIX="${CLICKHOUSE_TEST_UNIQUE_NAME}"

function cleanup()
{
    for suffix in settings other override plain computed; do
        ${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB}_${suffix}"
    done
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${USER}"
    ${CLICKHOUSE_CLIENT} --query "DROP SETTINGS PROFILE IF EXISTS ${PROFILE}"
    ${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION IF EXISTS ${NC}"
    ${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION IF EXISTS ${NC_OTHER}"
}
trap cleanup EXIT

function backup_of()
{
    echo "Disk('backups', '${BACKUP_PREFIX}_$1')"
}

# Creates the database `${DB}_$1` with the given engine, backs it up and drops it.
function make_backup()
{
    ${CLICKHOUSE_CLIENT} --query "
        CREATE DATABASE ${DB}_$1 ENGINE = $2;
        BACKUP DATABASE ${DB}_$1 TO $(backup_of "$1") FORMAT Null;
        DROP DATABASE ${DB}_$1;"
}

function restore_error()
{
    local name=$1
    local on_cluster=$2
    ${CLICKHOUSE_CLIENT} --user "${USER}" --query "
        RESTORE DATABASE ${DB}_${name} ${on_cluster} FROM $(backup_of "${name}") FORMAT Null" 2>&1 \
        | grep -o -m1 -E 'SETTING_CONSTRAINT_VIOLATION|ACCESS_DENIED|CANNOT_RESTORE_DATABASE'
}

function exists()
{
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.databases WHERE name = '${DB}_$1'"
}

function engine_arguments_of()
{
    ${CLICKHOUSE_CLIENT} --query "SELECT extract(engine_full, '\\\\((.*)\\\\)') FROM system.databases WHERE name = '${DB}_$1' FORMAT TSVRaw" \
        | sed "s/${NC}/nc/"
}

${CLICKHOUSE_CLIENT} --query "
    CREATE NAMED COLLECTION ${NC} AS ${NC_KEYS};
    CREATE NAMED COLLECTION ${NC_OTHER} AS ${NC_KEYS};
    CREATE SETTINGS PROFILE ${PROFILE} SETTINGS postgresql_connection_pool_size = 3 MAX 5;
    CREATE USER ${USER} SETTINGS PROFILE '${PROFILE}';
    GRANT CREATE DATABASE ON ${DB}_settings.* TO ${USER};
    GRANT CREATE DATABASE ON ${DB}_other.* TO ${USER};
    GRANT CREATE DATABASE ON ${DB}_override.* TO ${USER};
    GRANT CREATE DATABASE ON ${DB}_plain.* TO ${USER};
    GRANT POSTGRES, CLUSTER ON *.* TO ${USER};
    GRANT READ ON DISK TO ${USER};
    GRANT NAMED COLLECTION ON ${NC} TO ${USER};"

make_backup settings "PostgreSQL(${NC}) SETTINGS postgresql_connection_pool_size = 100"
make_backup other "PostgreSQL(${NC_OTHER})"
make_backup override "PostgreSQL(${NC}, host = '127.0.0.2')"
make_backup plain "PostgreSQL(${NC}, schema = 'public')"

echo "--- the user's constraints, the access to the named collection and the override of its keys, also ON CLUSTER"
for name in settings other override; do
    restore_error "${name}" ""
    restore_error "${name}" "ON CLUSTER test_shard_localhost"
    exists "${name}"
done
${CLICKHOUSE_CLIENT} --user "${USER}" --query "
    RESTORE DATABASE ${DB}_plain ON CLUSTER test_shard_localhost FROM $(backup_of plain) FORMAT Null"
engine_arguments_of plain

echo "--- a computed key of an override in the backup"
# Such a key is now replaced at `CREATE`, so the backup is edited into one with the expression, which databases created before
# keep. The backup is in S3 to edit it with a query. The replaced text has the same size, which the metadata of the backup records.
S3_BACKUP="http://localhost:11111/test/backups/${BACKUP_PREFIX}_computed"
S3_METADATA="s3('${S3_BACKUP}/metadata/${DB}_computed.sql', 'test', 'testtest', 'RawBLOB')"
${CLICKHOUSE_CLIENT} --query "
    CREATE DATABASE ${DB}_computed ENGINE = PostgreSQL(${NC}, schema = 'publicxxxxxxxxxxxxxx');
    BACKUP DATABASE ${DB}_computed TO S3('${S3_BACKUP}', 'test', 'testtest') FORMAT Null;
    DROP DATABASE ${DB}_computed;
    INSERT INTO FUNCTION ${S3_METADATA}
        SELECT replaceOne(raw_blob, 'schema = \\'publicxxxxxxxxxxxxxx\\'', 'concat(\\'sch\\', \\'ema\\') = \\'public\\'')
        FROM ${S3_METADATA}
        SETTINGS s3_truncate_on_insert = 1;"
${CLICKHOUSE_CLIENT} --query "
    RESTORE DATABASE ${DB}_computed ON CLUSTER test_shard_localhost FROM S3('${S3_BACKUP}', 'test', 'testtest') FORMAT Null" 2>&1 \
    | grep -o -m1 'CANNOT_RESTORE_DATABASE'
exists computed
${CLICKHOUSE_CLIENT} --query "RESTORE DATABASE ${DB}_computed FROM S3('${S3_BACKUP}', 'test', 'testtest') FORMAT Null"
engine_arguments_of computed
