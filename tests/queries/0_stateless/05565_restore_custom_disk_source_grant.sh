#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree
# Tag no-fasttest: needs MinIO
# Tag no-replicated-database: creates and restores databases
# Tag no-shared-merge-tree: a database metadata disk, as in 05099_create_database_with_inline_disk

# `RESTORE` creates tables and databases with internal queries, so it checks the source grants of the custom disks
# in the backed up definitions itself, as `CREATE` does.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}"
USER_NAME="u_05565_${DB}"
SOURCE_DB="${DB}_05565_source"
RESTORED_DB="${DB}_05565_restored"
TABLE_BACKUP="Disk('backups', '05565_table_${CLICKHOUSE_TEST_UNIQUE_NAME}')"
DATABASE_BACKUP="Disk('backups', '05565_database_${CLICKHOUSE_TEST_UNIQUE_NAME}')"

function s3_disk()
{
    echo "disk(name = '05565_$1_${DB}', type = object_storage, object_storage_type = s3,
        endpoint = 'http://localhost:11111/test/05565_${DB}/$1/', access_key_id = clickhouse, secret_access_key = clickhouse)"
}

# Runs the statement as the user and prints the grant it lacks, or `allowed` if no grant is missing.
function as_user()
{
    $CLICKHOUSE_CLIENT --user "${USER_NAME}" "${@:2}" -q "$1" 2>&1 \
        | grep -o -m1 -E "Not enough privileges. To execute this query, it's necessary to have the grant [^.]*" \
        | sed -E 's/.*necessary to have the grant //' \
        | grep . || echo "allowed"
}

function cleanup()
{
    $CLICKHOUSE_CLIENT -m -q "
        DROP TABLE IF EXISTS t_05565 SYNC;
        DROP TABLE IF EXISTS t_05565_restored SYNC;
        DROP DATABASE IF EXISTS ${SOURCE_DB} SYNC;
        DROP DATABASE IF EXISTS ${RESTORED_DB} SYNC;
        DROP USER IF EXISTS ${USER_NAME};"
}
trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT -m -q "
    CREATE TABLE t_05565 (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS disk = $(s3_disk table);
    INSERT INTO t_05565 SELECT number FROM numbers(10);
    CREATE DATABASE ${SOURCE_DB} SETTINGS disk = $(s3_disk database);"
$CLICKHOUSE_CLIENT -q "BACKUP TABLE t_05565 TO ${TABLE_BACKUP} FORMAT Null"
$CLICKHOUSE_CLIENT -q "BACKUP DATABASE ${SOURCE_DB} TO ${DATABASE_BACKUP} FORMAT Null"

$CLICKHOUSE_CLIENT -m -q "
    CREATE USER ${USER_NAME} IDENTIFIED WITH no_password;
    GRANT CREATE TABLE, SELECT, INSERT ON ${DB}.* TO ${USER_NAME};
    GRANT CREATE DATABASE ON ${RESTORED_DB}.* TO ${USER_NAME};
    GRANT TABLE ENGINE ON MergeTree TO ${USER_NAME};
    GRANT READ ON DISK TO ${USER_NAME};"

RESTORE_TABLE="RESTORE TABLE ${DB}.t_05565 AS ${DB}.t_05565_restored FROM ${TABLE_BACKUP} FORMAT Null"

echo "table"
as_user "${RESTORE_TABLE}" --restore_replace_external_engines_to_null=0
as_user "${RESTORE_TABLE}" --restore_replace_external_engines_to_null=1
$CLICKHOUSE_CLIENT -q "EXISTS TABLE t_05565_restored"

echo "database"
as_user "RESTORE DATABASE ${SOURCE_DB} AS ${RESTORED_DB} FROM ${DATABASE_BACKUP} FORMAT Null"
$CLICKHOUSE_CLIENT -q "EXISTS DATABASE ${RESTORED_DB}"

echo "database that exists now"
$CLICKHOUSE_CLIENT -q "CREATE DATABASE ${RESTORED_DB}"
as_user "RESTORE DATABASE ${SOURCE_DB} AS ${RESTORED_DB} FROM ${DATABASE_BACKUP} SETTINGS create_database = 'if-not-exists' FORMAT Null"
$CLICKHOUSE_CLIENT -q "DROP DATABASE ${RESTORED_DB} SYNC"

echo "with the S3 grant"
$CLICKHOUSE_CLIENT -q "GRANT READ, WRITE ON S3 TO ${USER_NAME}"
as_user "${RESTORE_TABLE}" --restore_replace_external_engines_to_null=1
$CLICKHOUSE_CLIENT --user "${USER_NAME}" -q "SELECT count() FROM t_05565_restored"
as_user "RESTORE DATABASE ${SOURCE_DB} AS ${RESTORED_DB} FROM ${DATABASE_BACKUP} FORMAT Null"
$CLICKHOUSE_CLIENT -q "EXISTS DATABASE ${RESTORED_DB}"
