#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database
# Tag zookeeper: `ON CLUSTER` queries use the distributed DDL queue.
# Tag no-replicated-database: on a replicated database the DDL runs with no user, so a local restore of a view
# is not refused for the engine of its inner table.

# The hosts of `RESTORE ... ON CLUSTER` create the restored tables without the user, so the initiator requires
# `TABLE ENGINE` on every engine the hosts will create, as a restore without `ON CLUSTER` does.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

mkdir -p "${CLICKHOUSE_USER_FILES_UNIQUE}"
printf 'canary\n' > "${CLICKHOUSE_USER_FILES_UNIQUE}/data.txt"
touch "${CLICKHOUSE_USER_FILES_UNIQUE}/mv.txt" "${CLICKHOUSE_USER_FILES_UNIQUE}/rmv.txt"
chmod 666 "${CLICKHOUSE_USER_FILES_UNIQUE}"/*.txt

db="${CLICKHOUSE_DATABASE}"
user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
backup="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"
cluster="ON CLUSTER test_shard_localhost"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${user};
CREATE USER ${user} IDENTIFIED WITH no_password;
-- DROP TABLE: the definer of a refreshable view needs it to create the view.
GRANT CREATE TABLE, CREATE VIEW, DROP TABLE, SELECT, INSERT ON ${db}.* TO ${user};
GRANT CLUSTER ON *.* TO ${user};
GRANT READ ON DISK TO ${user};
GRANT TABLE ENGINE ON Null TO ${user};
CREATE TABLE ${db}.src (line String) ENGINE = Memory;
CREATE TABLE ${db}.t_engine (line String) ENGINE = File(LineAsString, '${CLICKHOUSE_USER_FILES_UNIQUE}/data.txt');
CREATE MATERIALIZED VIEW ${db}.mv_file ENGINE = File(LineAsString, '${CLICKHOUSE_USER_FILES_UNIQUE}/mv.txt')
    AS SELECT line FROM ${db}.src;
CREATE MATERIALIZED VIEW ${db}.rmv_file REFRESH EVERY 1 YEAR
    ENGINE = File(LineAsString, '${CLICKHOUSE_USER_FILES_UNIQUE}/rmv.txt') EMPTY
    DEFINER = ${user} SQL SECURITY DEFINER AS SELECT line FROM ${db}.src;
BACKUP TABLE ${db}.src, TABLE ${db}.t_engine, TABLE ${db}.mv_file, TABLE ${db}.rmv_file TO ${backup} FORMAT Null;
"

# Prints the grant that the query of the user lacks, or `restored`. The other arguments go to the client.
function restore()
{
    local output query=$1
    shift
    output=$(${CLICKHOUSE_CLIENT} --user "${user}" --distributed_ddl_output_mode throw "$@" -q "${query}" 2>&1)
    if [ -z "${output}" ]; then
        echo 'restored'
    else
        echo "${output}" | grep -o -m1 -E 'grant [A-Z ]+ ON [A-Za-z]+' || echo "${output}"
    fi
}

echo '--- without a FILE grant, no File table is restored, neither locally nor on the cluster'
for t in t_engine mv_file rmv_file; do
    restore "RESTORE TABLE ${db}.${t} AS ${db}.r_${t}_local FROM ${backup} FORMAT Null"
    restore "RESTORE TABLE ${db}.${t} AS ${db}.r_${t}_cluster ${cluster} FROM ${backup} FORMAT Null"
done

echo '--- a host may drop a session value of restore_replace_external_engines_to_null, so it exempts no engine'
restore "RESTORE TABLE ${db}.t_engine AS ${db}.r_t_engine_session ${cluster} FROM ${backup}
    SETTINGS allow_different_table_def = 1 FORMAT Null" --restore_replace_external_engines_to_null 1

echo '--- no table was restored'
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = '${db}' AND startsWith(name, 'r_')"

echo '--- with restore_replace_external_engines_to_null, the hosts create Null tables, which need only TABLE ENGINE ON Null'
for t in t_engine mv_file rmv_file; do
    restore "RESTORE TABLE ${db}.${t} AS ${db}.r_${t}_null ${cluster} FROM ${backup}
        SETTINGS restore_replace_external_engines_to_null = 1, allow_different_table_def = 1 FORMAT Null"
done
${CLICKHOUSE_CLIENT} -q "
SELECT engine FROM system.tables WHERE database = '${db}' AND name = 'r_t_engine_null';
SELECT v.name, i.engine FROM system.tables AS v
    INNER JOIN system.tables AS i ON i.database = v.database AND i.name = concat('.inner_id.', toString(v.uuid))
    WHERE v.database = '${db}' AND v.name IN ('r_mv_file_null', 'r_rmv_file_null') ORDER BY v.name;
"

echo '--- with READ and WRITE ON FILE, the tables are restored on the cluster'
${CLICKHOUSE_CLIENT} -q "GRANT READ, WRITE ON FILE TO ${user}"
for t in t_engine mv_file rmv_file; do
    restore "RESTORE TABLE ${db}.${t} AS ${db}.r_${t} ${cluster} FROM ${backup} FORMAT Null"
done
${CLICKHOUSE_CLIENT} --user "${user}" -q "SELECT * FROM ${db}.r_t_engine"

${CLICKHOUSE_CLIENT} -q "
DROP TABLE ${db}.r_rmv_file_null;
DROP TABLE ${db}.r_mv_file_null;
DROP TABLE ${db}.r_t_engine_null;
DROP TABLE ${db}.r_rmv_file;
DROP TABLE ${db}.r_mv_file;
DROP TABLE ${db}.r_t_engine;
DROP TABLE ${db}.rmv_file;
DROP TABLE ${db}.mv_file;
DROP TABLE ${db}.t_engine;
DROP TABLE ${db}.src;
DROP USER ${user};
"
rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE}"
