#!/usr/bin/env bash
# Tags: zookeeper
# Tag zookeeper: `ON CLUSTER` queries use the distributed DDL queue.

# A restored database is created by an internal query, so `RESTORE` asks `TABLE ENGINE ON <engine>` like a
# `CREATE DATABASE` does, unless the restore skips the database. `Filesystem` is granted implicitly with
# `READ, WRITE ON FILE`, whatever `table_engines_require_grant` says. Under `ON CLUSTER` each host decides the
# skip itself, and an older one creates the database, so its engine is asked anyway.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}"
user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
bk_fs="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_fs')"
bk_atomic="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_atomic')"
cluster="ON CLUSTER test_shard_localhost"
# Entry format 1 carries no session settings, so only a value in the query's SETTINGS reaches the host.
format1=(--distributed_ddl_entry_format_version 1)

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${user};
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
        echo "denied: $(grep -oE 'grant [A-Z ]+ ON [^ ]+\.( |$)' <<< "${out}" | head -1 | sed -E 's/^grant //; s/\.( )?$//' | sed "s/${db}/db/g")"
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

for i in 1 2 3 4 5 6 7; do
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS ${db}_r${i}"
done
${CLICKHOUSE_CLIENT} -q "
DROP DATABASE IF EXISTS ${db}_fs;
DROP DATABASE IF EXISTS ${db}_atomic;
DROP USER IF EXISTS ${user};
"
