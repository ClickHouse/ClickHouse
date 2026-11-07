#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest
# Tag zookeeper: `ON CLUSTER` queries use the distributed DDL queue, and `S3Queue` keeps its state in Keeper.
# Tag no-fasttest: the `S3Queue` engine is not built into the fast test build.

# A restored database is created by an internal query, so `RESTORE` asks `TABLE ENGINE ON <engine>` like a
# `CREATE DATABASE` does, unless the restore skips the database. `Filesystem` is granted implicitly with
# `READ, WRITE ON FILE`, whatever `table_engines_require_grant` says. Under `ON CLUSTER` each host decides the
# skip itself, and an older one creates the database, so nothing is lifted for a database: neither its engine, nor
# the locator of a `Backup` database, nor the targets of a view. A table's export settings are lifted by a value in
# the query, the only one that reaches every host, and every host replaces the table's engine by `Null`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}"
user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
user_ext="user_ext_${CLICKHOUSE_TEST_UNIQUE_NAME}"
user_queue="user_queue_${CLICKHOUSE_TEST_UNIQUE_NAME}"
bk_inner="File('${CLICKHOUSE_TEST_UNIQUE_NAME}/inner')"
bk_backup="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_backup')"
bk_view="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_view')"
bk_queue="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_queue')"
bk_fs="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_fs')"
bk_atomic="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_atomic')"
cluster="ON CLUSTER test_shard_localhost"
# Entry format 1 carries no session settings, so only a value in the query's SETTINGS reaches the host.
format1=(--distributed_ddl_entry_format_version 1)

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${user}, ${user_ext}, ${user_queue};
DROP DATABASE IF EXISTS ${db}_fs;
DROP DATABASE IF EXISTS ${db}_atomic;
CREATE DATABASE ${db}_fs ENGINE = Filesystem;
CREATE DATABASE ${db}_atomic ENGINE = Atomic;
BACKUP DATABASE ${db}_fs TO ${bk_fs} FORMAT Null;
BACKUP DATABASE ${db}_atomic TO ${bk_atomic} FORMAT Null;
CREATE USER ${user};
GRANT CREATE DATABASE ON ${db}_r1.* TO ${user};
GRANT CREATE DATABASE ON ${db}_r2.* TO ${user};
GRANT CREATE DATABASE ON ${db}_r3.* TO ${user};
GRANT CREATE DATABASE ON ${db}_r4.* TO ${user};
GRANT CREATE DATABASE ON ${db}_r5.* TO ${user};
GRANT CREATE DATABASE ON ${db}_r6.* TO ${user};
GRANT CREATE DATABASE ON ${db}_r7.* TO ${user};
GRANT CLUSTER ON *.* TO ${user};
GRANT READ ON DISK TO ${user};
"

# Prints `allowed`, `denied: <the first missing grant>`, or the error code of another failure.
function check()
{
    local user=$1 query=$2 out
    shift 2
    if out=$(${CLICKHOUSE_CLIENT} --user "${user}" --distributed_ddl_output_mode throw "$@" -q "${query}" 2>&1); then
        echo 'allowed'
    elif grep -q 'ACCESS_DENIED' <<< "${out}"; then
        echo "denied: $(grep -oE 'grant [A-Z, ]+ ON [^ ]+\.( |$)' <<< "${out}" | head -1 | sed -E 's/^grant //; s/\.( )?$//' | sed "s/${db}/db/g")"
    else
        echo "error: $(grep -oE 'Code: [0-9]+' <<< "${out}" | head -1)"
    fi
}

function databases()
{
    ${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.databases WHERE name = '$1'"
}

echo '--- a database engine needs TABLE ENGINE, to create and to restore'
check "${user}" "CREATE DATABASE ${db}_r1 ENGINE = Filesystem"
check "${user}" "RESTORE DATABASE ${db}_fs AS ${db}_r2 FROM ${bk_fs} FORMAT Null"
databases "${db}_r2"

echo '--- a database created without an engine is Atomic, which is not checked'
check "${user}" "CREATE DATABASE ${db}_r3"
check "${user}" "CREATE DATABASE ${db}_r1 ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${db}_r3"
check "${user}" "RESTORE DATABASE ${db}_atomic AS ${db}_r3 FROM ${bk_atomic} FORMAT Null"

echo '--- also when the database exists'
${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${db}_r4 ENGINE = Filesystem"
check "${user}" "RESTORE DATABASE ${db}_fs AS ${db}_r4 FROM ${bk_fs}
    SETTINGS create_database = 'if-not-exists', allow_different_database_def = 1 FORMAT Null"

echo '--- on the cluster, a session value of restore_replace_external_engines_to_null skips nothing'
check "${user}" "RESTORE DATABASE ${db}_fs AS ${db}_r5 ${cluster} FROM ${bk_fs} FORMAT Null" \
    --restore_replace_external_engines_to_null 1 "${format1[@]}"
echo '--- nor does a value in the query: an older host would create the database'
check "${user}" "RESTORE DATABASE ${db}_fs AS ${db}_r6 ${cluster} FROM ${bk_fs}
    SETTINGS restore_replace_external_engines_to_null = 1 FORMAT Null" "${format1[@]}"
databases "${db}_r6"
echo '--- without ON CLUSTER a value in the query skips the database, which is not created'
check "${user}" "RESTORE DATABASE ${db}_fs AS ${db}_r6 FROM ${bk_fs} SETTINGS restore_replace_external_engines_to_null = 1 FORMAT Null"
databases "${db}_r6"

echo '--- READ and WRITE ON FILE grant the Filesystem engine'
${CLICKHOUSE_CLIENT} -q "GRANT READ, WRITE ON FILE TO ${user}"
check "${user}" "RESTORE DATABASE ${db}_fs AS ${db}_r7 FROM ${bk_fs} FORMAT Null"
databases "${db}_r7"

${CLICKHOUSE_CLIENT} -q "
CREATE DATABASE ${db}_inner_src ENGINE = Atomic;
CREATE TABLE ${db}_inner_src.t (x UInt64) ENGINE = MergeTree ORDER BY x;
BACKUP DATABASE ${db}_inner_src TO ${bk_inner} FORMAT Null;
CREATE DATABASE ${db}_bk ENGINE = Backup('${db}_inner_src', ${bk_inner});
BACKUP DATABASE ${db}_bk TO ${bk_backup} FORMAT Null;
CREATE TABLE ${db}.src (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE ${db}.own (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE ${db}.dst (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE MATERIALIZED VIEW ${db}.mv_own TO ${db}.own AS SELECT k FROM ${db}.src;
BACKUP DATABASE ${db}_fs, TABLE ${db}.mv_own AS ${db}_fs.mv, TABLE ${db}.own AS ${db}.dst TO ${bk_view}
    SETTINGS structure_only = 1 FORMAT Null;
CREATE DATABASE ${db}_r9 ENGINE = Atomic;
CREATE TABLE ${db}.queue (x UInt8) ENGINE = S3Queue('http://localhost:11111/test/${db}_queue_src/', 'clickhouse', 'clickhouse', 'CSV')
    SETTINGS mode = 'unordered', keeper_path = '/${CLICKHOUSE_TEST_UNIQUE_NAME}_queue', after_processing = 'move',
        after_processing_move_uri = 'http://localhost:11111/test/${db}_queue_dst/',
        after_processing_move_access_key_id = 'clickhouse', after_processing_move_secret_access_key = 'clickhouse';
BACKUP TABLE ${db}.queue TO ${bk_queue} FORMAT Null;
CREATE USER ${user_ext}, ${user_queue};
-- The wildcard is the only spelling that reaches the Backup database engine; it confers no source grant.
GRANT TABLE ENGINE ON *.* TO ${user_ext};
GRANT CREATE DATABASE, CREATE TABLE, SELECT ON ${db}_r8.* TO ${user_ext};
GRANT CREATE DATABASE, CREATE TABLE, CREATE VIEW ON ${db}_r9.* TO ${user_ext};
GRANT CREATE TABLE ON ${db}.* TO ${user_queue};
GRANT TABLE ENGINE ON S3Queue TO ${user_queue};
GRANT CLUSTER ON *.* TO ${user_ext}, ${user_queue};
GRANT READ ON DISK TO ${user_ext}, ${user_queue};
"

echo '--- on the cluster, a session value does not lift the other checks either'
check "${user_ext}" "RESTORE DATABASE ${db}_bk AS ${db}_r8 ${cluster} FROM ${bk_backup}
    SETTINGS structure_only = 1 FORMAT Null" --restore_replace_external_engines_to_null 1 "${format1[@]}"
check "${user_ext}" "RESTORE DATABASE ${db}_fs AS ${db}_r9 ${cluster} FROM ${bk_view}
    SETTINGS allow_different_database_def = 1 FORMAT Null" --restore_replace_external_engines_to_null 1 "${format1[@]}"
check "${user_queue}" "RESTORE TABLE ${db}.queue AS ${db}.queue_restored ${cluster} FROM ${bk_queue}
    SETTINGS structure_only = 1, allow_different_table_def = 1 FORMAT Null" --restore_replace_external_engines_to_null 1 "${format1[@]}"
echo '--- nor does a value in the query lift the checks of a database'
check "${user_ext}" "RESTORE DATABASE ${db}_bk AS ${db}_r8 ${cluster} FROM ${bk_backup}
    SETTINGS structure_only = 1, restore_replace_external_engines_to_null = 1 FORMAT Null" "${format1[@]}"
check "${user_ext}" "RESTORE DATABASE ${db}_fs AS ${db}_r9 ${cluster} FROM ${bk_view}
    SETTINGS allow_different_database_def = 1, restore_replace_external_engines_to_null = 1 FORMAT Null" "${format1[@]}"
echo '--- a value in the query lifts the export settings of a table, as every host replaces its engine by Null'
check "${user_queue}" "RESTORE TABLE ${db}.queue AS ${db}.queue_q ${cluster} FROM ${bk_queue}
    SETTINGS structure_only = 1, allow_different_table_def = 1, restore_replace_external_engines_to_null = 1 FORMAT Null" "${format1[@]}"
${CLICKHOUSE_CLIENT} -q "SELECT engine FROM system.tables WHERE database = '${db}' AND name = 'queue_q'"

for i in 1 2 3 4 5 6 7 8 9; do
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${db}_r${i}"
done
${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS ${db}.queue_restored SYNC;
DROP TABLE IF EXISTS ${db}.queue_q SYNC;
DROP TABLE IF EXISTS ${db}.queue SYNC;
DROP TABLE IF EXISTS ${db}.mv_own;
DROP DATABASE IF EXISTS ${db}_bk;
DROP DATABASE IF EXISTS ${db}_inner_src;
DROP DATABASE IF EXISTS ${db}_fs;
DROP DATABASE IF EXISTS ${db}_atomic;
DROP USER IF EXISTS ${user}, ${user_ext}, ${user_queue};
"
