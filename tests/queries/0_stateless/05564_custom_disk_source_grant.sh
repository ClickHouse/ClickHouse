#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree
# Tag no-fasttest: needs MinIO
# Tag no-replicated-database: ON CLUSTER arms, and arms that create databases
# Tag no-shared-merge-tree: a database metadata disk, as in 05099_create_database_with_inline_disk

# Defining a disk in SQL needs the SOURCES grant of every backend it declares, like the table functions do.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}"
USER_NAME="u_05564_${DB}"
OTHER_DB="${DB}_05564_e"

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

function disk_exists()
{
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.disks WHERE name = '05564_$1_${DB}'"
}

function cleanup()
{
    $CLICKHOUSE_CLIENT -m -q "
        DROP TABLE IF EXISTS t_05564_a SYNC;
        DROP TABLE IF EXISTS t_05564_b SYNC;
        DROP TABLE IF EXISTS t_05564_c SYNC;
        DROP TABLE IF EXISTS ${DB}.t_05564_d SYNC;
        DROP TABLE IF EXISTS ${DB}.t_05564_d2 SYNC;
        DROP TABLE IF EXISTS ${DB}.mv_05564_d3 SYNC;
        DROP TABLE IF EXISTS t_05564_i SYNC;
        DROP TABLE IF EXISTS t_05564_j SYNC;
        DROP TABLE IF EXISTS t_05564_k SYNC;
        DROP TABLE IF EXISTS t_05564_l SYNC;
        DROP TABLE IF EXISTS t_05564_m SYNC;
        DROP TABLE IF EXISTS t_05564_n SYNC;
        DROP TABLE IF EXISTS t_05564_o SYNC;
        DROP TABLE IF EXISTS t_05564_p SYNC;
        DROP TABLE IF EXISTS t_05564_q SYNC;
        DROP TABLE IF EXISTS t_05564_s SYNC;
        DROP TABLE IF EXISTS t_05564_u SYNC;
        DROP TABLE IF EXISTS t_05564_z SYNC;
        DROP FUNCTION IF EXISTS disk_05564_${DB};
        DROP DATABASE IF EXISTS ${OTHER_DB} SYNC;
        DROP USER IF EXISTS ${USER_NAME};"
}
trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT -m -q "
    CREATE USER ${USER_NAME} IDENTIFIED WITH no_password;
    GRANT CREATE TABLE, ALTER SETTINGS, SELECT, INSERT, DROP TABLE ON ${DB}.* TO ${USER_NAME};
    GRANT CREATE VIEW ON ${DB}.* TO ${USER_NAME};
    GRANT TABLE ENGINE ON MergeTree TO ${USER_NAME};"

TABLE="(id UInt64) ENGINE = MergeTree ORDER BY id"

echo "A: s3 disk"
as_user "CREATE TABLE t_05564_a ${TABLE} SETTINGS disk = $(s3_disk a)"
$CLICKHOUSE_CLIENT -q "EXISTS TABLE t_05564_a"
disk_exists a

echo "B: s3 disk inside a cache disk"
as_user "CREATE TABLE t_05564_b ${TABLE} SETTINGS disk = disk(name = '05564_b_cache_${DB}', type = cache,
    max_size = '100Ki', max_file_segment_size = '32Ki', boundary_alignment = '32Ki', path = '05564_b_${DB}', disk = $(s3_disk b))"
disk_exists b

echo "H: local disk"
as_user "CREATE TABLE t_05564_c ${TABLE} SETTINGS disk = disk(type = local, path = '${CLICKHOUSE_DISKS_FILES}/05564_c_${DB}/')"
$CLICKHOUSE_CLIENT -q "EXISTS TABLE t_05564_c"

echo "C: ALTER to an s3 disk"
as_user "ALTER TABLE t_05564_c MODIFY SETTING disk = $(s3_disk c)"
disk_exists c

echo "D: ON CLUSTER"
$CLICKHOUSE_CLIENT -q "GRANT CLUSTER ON *.* TO ${USER_NAME}"
as_user "CREATE TABLE ${DB}.t_05564_d ON CLUSTER test_shard_localhost ${TABLE} SETTINGS disk = $(s3_disk d)" \
    --distributed_ddl_output_mode=none
disk_exists d

echo "D2: ON CLUSTER, legacy entry format, engine from default_table_engine"
as_user "CREATE TABLE ${DB}.t_05564_d2 ON CLUSTER test_shard_localhost (id UInt64) ORDER BY id SETTINGS disk = $(s3_disk d2)" \
    --distributed_ddl_entry_format_version=2 --distributed_ddl_output_mode=none
disk_exists d2

echo "D3: the same for the inner table of a materialized view"
as_user "CREATE MATERIALIZED VIEW ${DB}.mv_05564_d3 ON CLUSTER test_shard_localhost ORDER BY id
    SETTINGS disk = $(s3_disk d3) AS SELECT id FROM ${DB}.t_05564_c" \
    --distributed_ddl_entry_format_version=2 --distributed_ddl_output_mode=none
disk_exists d3

echo "E: database metadata disk"
$CLICKHOUSE_CLIENT -q "GRANT CREATE DATABASE ON ${OTHER_DB}.* TO ${USER_NAME}"
as_user "CREATE DATABASE ${OTHER_DB} SETTINGS disk = $(s3_disk e)"
$CLICKHOUSE_CLIENT -q "EXISTS DATABASE ${OTHER_DB}"

echo "F: projection settings do not build a disk"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_05564_c ADD PROJECTION p (SELECT id ORDER BY id) WITH SETTINGS (disk = $(s3_disk f))" 2>&1 \
    | grep -o -m1 -F 'BAD_ARGUMENTS'
disk_exists f

echo "J: cache over a configured disk"
as_user "CREATE TABLE t_05564_j ${TABLE} SETTINGS disk = disk(name = '05564_j_${DB}', type = cache,
    max_size = '100Ki', max_file_segment_size = '32Ki', boundary_alignment = '32Ki', path = '05564_j_${DB}', disk = s3_disk)"
$CLICKHOUSE_CLIENT --user "${USER_NAME}" -q "INSERT INTO t_05564_j VALUES (1); SELECT count() FROM t_05564_j" -m

echo "K: web disk needs only READ ON URL"
WEB_TABLE="t_05564_k ${TABLE} SETTINGS disk = disk(type = web, endpoint = 'http://localhost:11111/test/05564_${DB}/k/')"
as_user "CREATE TABLE ${WEB_TABLE}"
$CLICKHOUSE_CLIENT -q "GRANT READ ON URL TO ${USER_NAME}"
as_user "CREATE TABLE ${WEB_TABLE}"
$CLICKHOUSE_CLIENT -q "REVOKE READ ON URL FROM ${USER_NAME}"

echo "L: repeated type"
as_user "CREATE TABLE t_05564_l ${TABLE} SETTINGS disk = disk(name = '05564_l_${DB}', type = s3,
    endpoint = 'http://localhost:11111/test/05564_${DB}/l/', access_key_id = clickhouse, secret_access_key = clickhouse,
    type = local, path = '${CLICKHOUSE_DISKS_FILES}/05564_l_${DB}/')"
disk_exists l

echo "N: unknown type"
as_user "CREATE TABLE t_05564_n ${TABLE} SETTINGS disk = disk(type = no_such_disk_type_05564, endpoint = 'http://localhost:11111/test/05564_${DB}/n/')"

echo "O: a key = value argument written as equals() with an extra operand"
as_user "CREATE TABLE t_05564_o ${TABLE} SETTINGS disk = disk(name = '05564_o_${DB}', equals(type, 's3', 0),
    endpoint = 'http://localhost:11111/test/05564_${DB}/o/', access_key_id = clickhouse, secret_access_key = clickhouse)"
disk_exists o

echo "P: the source each object storage type declares"
for storage_type in s3_with_keeper s3_plain s3_plain_rewritable azure_blob_storage azure azure_plain azure_plain_rewritable hdfs; do
    echo "${storage_type}: $(as_user "CREATE TABLE t_05564_p ${TABLE} SETTINGS disk = disk(type = object_storage,
        object_storage_type = ${storage_type}, endpoint = 'http://localhost:11111/test/05564_${DB}/p/')")"
done

echo "S: the types that declare no source"
for storage_type in local_blob_storage local_plain local_plain_rewritable; do
    echo "${storage_type}: $(as_user "CREATE TABLE t_05564_s ${TABLE} SETTINGS disk = disk(type = object_storage,
        object_storage_type = ${storage_type}, path = '${CLICKHOUSE_DISKS_FILES}/05564_s_${storage_type}_${DB}/')")"
    $CLICKHOUSE_CLIENT -q "EXISTS TABLE t_05564_s"
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t_05564_s SYNC"
done
echo "encrypted: $(as_user "CREATE TABLE t_05564_s ${TABLE} SETTINGS disk = disk(type = encrypted, key = '1234567812345678',
    path = '05564_s/', disk = disk(type = local, path = '${CLICKHOUSE_DISKS_FILES}/05564_s_encrypted_${DB}/'))")"
$CLICKHOUSE_CLIENT -q "EXISTS TABLE t_05564_s"

echo "U: a function named disk* outside a disk setting defines no disk"
$CLICKHOUSE_CLIENT -q "CREATE FUNCTION disk_05564_${DB} AS (x) -> x"
as_user "CREATE TABLE t_05564_u (id UInt64, type String) ENGINE = MergeTree ORDER BY (id, disk_05564_${DB}(type = 's3'))"
$CLICKHOUSE_CLIENT -q "EXISTS TABLE t_05564_u"

echo "G: with the S3 grant"
$CLICKHOUSE_CLIENT -q "GRANT READ, WRITE ON S3 TO ${USER_NAME}"
$CLICKHOUSE_CLIENT --user "${USER_NAME}" -m -q "
    CREATE TABLE t_05564_a ${TABLE} SETTINGS disk = $(s3_disk a);
    INSERT INTO t_05564_a SELECT number FROM numbers(10);
    SELECT count() FROM t_05564_a;"

echo "I: include needs the grant on every source, even with an s3 type"
as_user "CREATE TABLE t_05564_i ${TABLE} SETTINGS disk = disk(name = '05564_i_${DB}', type = s3,
    endpoint = 'http://localhost:11111/test/05564_${DB}/i/', access_key_id = clickhouse, secret_access_key = clickhouse,
    include = 'no_such_include_05564')" --dynamic_disk_allow_include=1
disk_exists i

echo "Z: a from_zk type needs the grant on every source, even with an s3 object storage"
as_user "CREATE TABLE t_05564_z ${TABLE} SETTINGS disk = disk(name = '05564_z_${DB}', type = 'from_zk /no_such_node_05564',
    object_storage_type = s3, endpoint = 'http://localhost:11111/test/05564_${DB}/z/',
    access_key_id = clickhouse, secret_access_key = clickhouse)" --dynamic_disk_allow_from_zk=1

echo "Q: a from_zk value can add locations, even with a local object storage"
as_user "CREATE TABLE t_05564_q ${TABLE} SETTINGS disk = disk(name = '05564_q_${DB}', type = object_storage,
    object_storage_type = local_blob_storage, path = '${CLICKHOUSE_DISKS_FILES}/05564_q_${DB}/',
    locations = 'from_zk /no_such_node_05564')" --dynamic_disk_allow_from_zk=1
disk_exists q

echo "M: a substituted type needs the grant on every source"
FROM_ENV_TABLE="t_05564_m ${TABLE} SETTINGS disk = disk(type = 'from_env NO_SUCH_VARIABLE_05564',
    endpoint = 'http://localhost:11111/test/05564_${DB}/m/', access_key_id = clickhouse, secret_access_key = clickhouse)"
as_user "CREATE TABLE ${FROM_ENV_TABLE}" --dynamic_disk_allow_from_env=1
$CLICKHOUSE_CLIENT -q "GRANT READ, WRITE ON * TO ${USER_NAME}"
as_user "CREATE TABLE ${FROM_ENV_TABLE}" --dynamic_disk_allow_from_env=1
