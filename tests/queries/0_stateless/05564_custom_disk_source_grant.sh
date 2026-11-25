#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree
# Tag no-fasttest: needs MinIO
# Tag no-replicated-database: ON CLUSTER arms, and arms that create databases
# Tag no-shared-merge-tree: a database metadata disk, as in 05099_create_database_with_inline_disk

# A table or a database on a disk defined in SQL needs the grant of the source the disk stores data in,
# like the table functions do.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}"
USER_NAME="u_05564_${DB}"
OTHER_DB="${DB}_05564_e"
BACKUP="Disk('backups', '05564_${CLICKHOUSE_TEST_UNIQUE_NAME}')"

function s3_disk()
{
    echo "disk(name = '05564_$1_${DB}', type = object_storage, object_storage_type = s3,
        endpoint = 'http://localhost:11111/test/05564_${DB}/$1/', access_key_id = clickhouse, secret_access_key = clickhouse)"
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
        DROP DATABASE IF EXISTS ${OTHER_DB} SYNC;
        DROP USER IF EXISTS ${USER_NAME};"
}
trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT -m -q "
    CREATE USER ${USER_NAME} IDENTIFIED WITH no_password;
    GRANT CREATE TABLE, ALTER SETTINGS, SELECT, INSERT, DROP TABLE ON ${DB}.* TO ${USER_NAME};
    GRANT TABLE ENGINE ON MergeTree TO ${USER_NAME};"

TABLE="(id UInt64) ENGINE = MergeTree ORDER BY id"

echo "A: s3 disk"
as_user "CREATE TABLE t_05564_a ${TABLE} SETTINGS disk = $(s3_disk a)"
$CLICKHOUSE_CLIENT -q "EXISTS TABLE t_05564_a"

echo "B: s3 disk inside a cache disk"
as_user "CREATE TABLE t_05564_b ${TABLE} SETTINGS disk = disk(name = '05564_b_cache_${DB}', type = cache,
    max_size = '100Ki', max_file_segment_size = '32Ki', boundary_alignment = '32Ki', path = '05564_b_${DB}', disk = $(s3_disk b))"
$CLICKHOUSE_CLIENT -q "EXISTS TABLE t_05564_b"

echo "H: local disk"
as_user "CREATE TABLE t_05564_c ${TABLE} SETTINGS disk = disk(type = local, path = '${CLICKHOUSE_DISKS_FILES}/05564_c_${DB}/')"
$CLICKHOUSE_CLIENT -q "EXISTS TABLE t_05564_c"

echo "C: ALTER to an s3 disk"
as_user "ALTER TABLE t_05564_c MODIFY SETTING disk = $(s3_disk c)"

echo "D: ON CLUSTER"
$CLICKHOUSE_CLIENT -q "GRANT CLUSTER ON *.* TO ${USER_NAME}"
as_user "CREATE TABLE ${DB}.t_05564_d ON CLUSTER test_shard_localhost ${TABLE} SETTINGS disk = $(s3_disk d)" \
    --distributed_ddl_output_mode=none
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.disks WHERE name = '05564_d_${DB}'"

echo "E: database metadata disk"
$CLICKHOUSE_CLIENT -q "GRANT CREATE DATABASE ON ${OTHER_DB}.* TO ${USER_NAME}"
as_user "CREATE DATABASE ${OTHER_DB} SETTINGS disk = $(s3_disk e)"
$CLICKHOUSE_CLIENT -q "EXISTS DATABASE ${OTHER_DB}"

echo "F: projection settings do not build a disk"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_05564_c ADD PROJECTION p (SELECT id ORDER BY id) WITH SETTINGS (disk = $(s3_disk f))" 2>&1 \
    | grep -o -m1 -F 'BAD_ARGUMENTS'
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.disks WHERE name = '05564_f_${DB}'"

echo "J: cache over a configured disk"
as_user "CREATE TABLE t_05564_j ${TABLE} SETTINGS disk = disk(name = '05564_j_${DB}', type = cache,
    max_size = '100Ki', max_file_segment_size = '32Ki', boundary_alignment = '32Ki', path = '05564_j_${DB}', disk = s3_disk)"
$CLICKHOUSE_CLIENT -q "EXISTS TABLE t_05564_j"

echo "R: RESTORE"
$CLICKHOUSE_CLIENT -m -q "
    CREATE TABLE t_05564_r ${TABLE} SETTINGS disk = $(s3_disk r);
    GRANT READ ON DISK TO ${USER_NAME};"
$CLICKHOUSE_CLIENT -q "BACKUP TABLE t_05564_r TO ${BACKUP} FORMAT Null"
RESTORE="RESTORE TABLE ${DB}.t_05564_r AS ${DB}.t_05564_r2 FROM ${BACKUP} FORMAT Null"
as_user "${RESTORE}" --restore_replace_external_engines_to_null=0
as_user "${RESTORE}" --restore_replace_external_engines_to_null=1
$CLICKHOUSE_CLIENT -q "EXISTS TABLE t_05564_r2"

echo "G: with the S3 grant"
$CLICKHOUSE_CLIENT -q "GRANT READ, WRITE ON S3 TO ${USER_NAME}"
$CLICKHOUSE_CLIENT --user "${USER_NAME}" -m -q "
    CREATE TABLE t_05564_a ${TABLE} SETTINGS disk = $(s3_disk a);
    INSERT INTO t_05564_a SELECT number FROM numbers(10);
    SELECT count() FROM t_05564_a;"
