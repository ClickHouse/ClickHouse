#!/usr/bin/env bash
# Tags: zookeeper
# Tag zookeeper: `ON CLUSTER` queries use the distributed DDL queue.

# The hosts of `RESTORE ... ON CLUSTER` open the base backups with no user, so the initiator requires the source grant
# of every base backup in the chain, as a restore without `ON CLUSTER` does.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}"
user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
cluster="ON CLUSTER test_shard_localhost"
base="File('${CLICKHOUSE_TEST_UNIQUE_NAME}/base')"
outer="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}/outer')"
mid="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}/mid')"
outer2="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}/outer2')"

# Each backup gets its own `.sql` (a different comment), so only the data part, stored in `base` alone, is read from a base.
${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${user};
CREATE USER ${user} IDENTIFIED WITH no_password;
GRANT CREATE TABLE, INSERT, SELECT ON ${db}.* TO ${user};
GRANT CLUSTER ON *.* TO ${user};
GRANT READ ON DISK TO ${user};
GRANT TABLE ENGINE ON MergeTree TO ${user};
CREATE TABLE ${db}.t (x UInt64) ENGINE = MergeTree ORDER BY x COMMENT 'level 0';
INSERT INTO ${db}.t VALUES (7);
BACKUP TABLE ${db}.t TO ${base} FORMAT Null;
ALTER TABLE ${db}.t MODIFY COMMENT 'level 1';
BACKUP TABLE ${db}.t TO ${outer} SETTINGS base_backup = ${base} FORMAT Null;
BACKUP TABLE ${db}.t TO ${mid} SETTINGS base_backup = ${base} FORMAT Null;
ALTER TABLE ${db}.t MODIFY COMMENT '';
BACKUP TABLE ${db}.t TO ${outer2} SETTINGS base_backup = ${mid} FORMAT Null;
"

# Prints the error code and the grant that the query of the user lacks, or `restored`.
function restore()
{
    local output
    output=$(${CLICKHOUSE_CLIENT} --user "${user}" --distributed_ddl_output_mode throw -q "$1" 2>&1)
    if [ -z "${output}" ]; then
        echo 'restored'
    else
        echo "$(echo "${output}" | grep -o -m1 -E 'Code: [0-9]+') $(echo "${output}" | grep -o -m1 -E 'grant [A-Z ]+ ON [A-Za-z]+')"
    fi
}

function restore_all()
{
    restore "RESTORE TABLE ${db}.t AS ${db}.r1 ${cluster} FROM ${outer} FORMAT Null"
    restore "RESTORE TABLE ${db}.t AS ${db}.r2 ${cluster} FROM ${outer2} FORMAT Null"
    restore "RESTORE TABLE ${db}.t AS ${db}.r3 ${cluster} FROM ${outer2} SETTINGS base_backup = ${mid} FORMAT Null"
}

echo '--- without READ ON FILE, a local restore reads no base'
restore "RESTORE TABLE ${db}.t AS ${db}.r_local FROM ${outer} FORMAT Null"

echo '--- without READ ON FILE, neither does a restore on the cluster: a base from the metadata, a base of a base, an explicit base'
restore_all
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.tables WHERE database = '${db}' AND name IN ('r1', 'r2', 'r3')"

echo '--- with READ ON FILE, the row stored only in the base is restored on the cluster'
${CLICKHOUSE_CLIENT} -q "GRANT READ ON FILE TO ${user}"
restore_all
for t in r1 r2 r3; do
    ${CLICKHOUSE_CLIENT} --user "${user}" -q "SELECT x FROM ${db}.${t}"
done

${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS ${db}.r_local;
DROP TABLE ${db}.r1;
DROP TABLE ${db}.r2;
DROP TABLE ${db}.r3;
DROP TABLE ${db}.t;
DROP USER ${user};
"
