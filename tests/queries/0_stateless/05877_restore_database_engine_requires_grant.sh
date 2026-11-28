#!/usr/bin/env bash
# Tags: zookeeper
# Tag zookeeper: `ON CLUSTER` queries use the distributed DDL queue.

# A restored database is created only with `TABLE ENGINE` on its engine, as `CREATE DATABASE` is. A database engine
# has no grant of its own, so only `TABLE ENGINE ON *` covers it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
src="${CLICKHOUSE_DATABASE}_src"
dst="${CLICKHOUSE_DATABASE}_dst"
backup="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${user};
CREATE USER ${user} IDENTIFIED WITH no_password;
GRANT CREATE DATABASE ON ${dst}.* TO ${user};
GRANT CLUSTER ON *.* TO ${user};
GRANT READ ON DISK TO ${user};
CREATE DATABASE ${src} ENGINE = Atomic;
BACKUP DATABASE ${src} TO ${backup} FORMAT Null;
"

# Prints the grant that the query of the user lacks, or `restored`.
function restore()
{
    local output
    output=$(${CLICKHOUSE_CLIENT} --user "${user}" --distributed_ddl_output_mode throw -q "$1" 2>&1)
    if [ -z "${output}" ]; then
        echo 'restored'
    else
        echo "${output}" | grep -o -m1 -E 'grant [A-Z ]+ ON [A-Za-z]+' || echo "${output}"
    fi
}

echo '--- without TABLE ENGINE ON Atomic'
restore "RESTORE DATABASE ${src} AS ${dst} FROM ${backup} FORMAT Null"
restore "RESTORE DATABASE ${src} AS ${dst} ON CLUSTER test_shard_localhost FROM ${backup} FORMAT Null"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.databases WHERE name = '${dst}'"

echo '--- with TABLE ENGINE ON *'
${CLICKHOUSE_CLIENT} -q "GRANT TABLE ENGINE ON * TO ${user}"
restore "RESTORE DATABASE ${src} AS ${dst} FROM ${backup} FORMAT Null"
${CLICKHOUSE_CLIENT} -q "SELECT engine FROM system.databases WHERE name = '${dst}'"
${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${dst}"
restore "RESTORE DATABASE ${src} AS ${dst} ON CLUSTER test_shard_localhost FROM ${backup} FORMAT Null"
${CLICKHOUSE_CLIENT} -q "SELECT engine FROM system.databases WHERE name = '${dst}'"

${CLICKHOUSE_CLIENT} -q "
DROP DATABASE ${dst};
DROP DATABASE ${src};
DROP USER ${user};
"
