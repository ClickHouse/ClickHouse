#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: that mode installs a replicated access storage, whose entity serialization round
# trip widens `GRANT WRITE ON FILE` to both `READ` and `WRITE`, so none of the denials below would fire.

# A table created by `CREATE TABLE ... AS file(...)` reads the file without checking the grants of the user
# who reads the table. So creating it requires `READ ON FILE` as well as `WRITE ON FILE`, as the `File` engine
# does, however the structure of the table is obtained, also when the table is created by the hosts of an
# `ON CLUSTER` query, which execute it without the user, unless those hosts replace the table function with
# `Null`, as `restore_replace_external_table_functions_to_null` makes a local `CREATE` do. The same holds for a
# short `ATTACH` and for `RESTORE`. A table over `loop(file(...))` reads the file under the reader instead, so
# its creation requires neither.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

mkdir -p "${CLICKHOUSE_USER_FILES_UNIQUE}"
data_file="${CLICKHOUSE_USER_FILES_UNIQUE}/data.txt"
printf 'canary\n' > "${data_file}"
chmod 666 "${data_file}"

db="${CLICKHOUSE_DATABASE}"
user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
file_function="file('${data_file}', 'LineAsString')"
backup="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${user};
CREATE USER ${user} IDENTIFIED WITH no_password;
GRANT CREATE TABLE, SELECT, INSERT ON ${db}.* TO ${user};
GRANT CREATE TEMPORARY TABLE, CLUSTER ON *.* TO ${user};
GRANT WRITE ON FILE TO ${user};
CREATE TABLE ${db}.src (line String) AS ${file_function};
"

# Prints the source grant that the query of the user lacks.
function missing_grant()
{
    ${CLICKHOUSE_CLIENT} --user "${user}" --distributed_ddl_output_mode throw "$@" 2>&1 |
        grep -o -m1 -E 'grant (READ|WRITE) ON FILE'
}

echo '--- with WRITE ON FILE only, reading the file is refused'
missing_grant -q "SELECT * FROM ${file_function}"

echo '--- and so is creating a table from it with an explicit structure'
missing_grant -q "CREATE TABLE ${db}.t_columns (line String) AS ${file_function}"

echo '--- with the structure inferred from the file'
missing_grant -q "CREATE TABLE ${db}.t_inferred AS ${file_function}"

echo '--- with the structure and the table function copied from another table'
missing_grant -q "CREATE TABLE ${db}.t_copy AS ${db}.src"

echo '--- with the table function wrapped in loop, the table is created, but reading it is refused'
${CLICKHOUSE_CLIENT} --user "${user}" -q "CREATE TABLE ${db}.t_loop (line String) AS loop(${file_function})"
missing_grant -q "SELECT * FROM ${db}.t_loop LIMIT 1"
${CLICKHOUSE_CLIENT} -q "DROP TABLE ${db}.t_loop"

echo '--- ON CLUSTER'
missing_grant -q "CREATE TABLE ${db}.t_cluster ON CLUSTER test_shard_localhost (line String) AS ${file_function}"

echo '--- ON CLUSTER with an entry format that ships the query as written'
missing_grant --distributed_ddl_entry_format_version 2 \
    -q "CREATE TABLE ${db}.t_cluster_legacy ON CLUSTER test_shard_localhost (line String) AS ${file_function}"
missing_grant --distributed_ddl_entry_format_version 2 \
    -q "CREATE TABLE ${db}.t_cluster_legacy_copy ON CLUSTER test_shard_localhost AS ${db}.src"

echo '--- no table was created'
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = '${db}' AND startsWith(name, 't_')"

echo '--- ON CLUSTER with the table function replaced by Null, if the entry format ships that setting to the hosts'
${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON Null TO ${user}"
missing_grant --distributed_ddl_entry_format_version 1 --restore_replace_external_table_functions_to_null 1 \
    -q "CREATE TABLE ${db}.t_cluster_null ON CLUSTER test_shard_localhost (line String) AS ${file_function}"
${CLICKHOUSE_CLIENT} --user "${user}" --distributed_ddl_output_mode throw \
    --distributed_ddl_entry_format_version 2 --restore_replace_external_table_functions_to_null 1 \
    -q "CREATE TABLE ${db}.t_cluster_null ON CLUSTER test_shard_localhost (line String) AS ${file_function}" > /dev/null
${CLICKHOUSE_CLIENT} -q "SELECT engine FROM system.tables WHERE database = '${db}' AND name = 't_cluster_null'"

echo '--- with READ ON FILE only, creating the table is refused too'
${CLICKHOUSE_CLIENT} -q "
REVOKE WRITE ON FILE FROM ${user};
GRANT READ ON FILE TO ${user};
"
missing_grant -q "CREATE TABLE ${db}.t_columns (line String) AS ${file_function}"

echo '--- but a table wrapped in loop is created and reads the file'
${CLICKHOUSE_CLIENT} --user "${user}" -q "CREATE TABLE ${db}.t_loop (line String) AS loop(${file_function})"
${CLICKHOUSE_CLIENT} --user "${user}" -q "SELECT * FROM ${db}.t_loop LIMIT 1"

echo '--- with both, the table is created and reads the file'
${CLICKHOUSE_CLIENT} -q "GRANT WRITE ON FILE TO ${user}"
${CLICKHOUSE_CLIENT} --user "${user}" -q "CREATE TABLE ${db}.t_columns (line String) AS ${file_function}"
${CLICKHOUSE_CLIENT} --user "${user}" -q "SELECT * FROM ${db}.t_columns"
${CLICKHOUSE_CLIENT} --user "${user}" --distributed_ddl_output_mode throw \
    -q "CREATE TABLE ${db}.t_cluster ON CLUSTER test_shard_localhost (line String) AS ${file_function}" > /dev/null
${CLICKHOUSE_CLIENT} --user "${user}" -q "SELECT * FROM ${db}.t_cluster"

echo '--- without READ ON FILE, the table is neither attached back from its stored definition'
${CLICKHOUSE_CLIENT} -q "
REVOKE READ ON FILE FROM ${user};
DETACH TABLE ${db}.t_columns;
"
missing_grant -q "ATTACH TABLE ${db}.t_columns"
${CLICKHOUSE_CLIENT} -q "ATTACH TABLE ${db}.t_columns"

echo '--- nor restored from a backup'
${CLICKHOUSE_CLIENT} -q "BACKUP TABLE ${db}.t_columns TO ${backup} FORMAT Null"
${CLICKHOUSE_CLIENT} -q "GRANT READ ON DISK TO ${user}"
missing_grant -q "RESTORE TABLE ${db}.t_columns AS ${db}.t_restored FROM ${backup} FORMAT Null"

${CLICKHOUSE_CLIENT} -q "
DROP TABLE ${db}.t_columns;
DROP TABLE ${db}.t_cluster;
DROP TABLE ${db}.t_cluster_null;
DROP TABLE ${db}.t_loop;
DROP TABLE ${db}.src;
DROP USER ${user};
"
rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE}"
