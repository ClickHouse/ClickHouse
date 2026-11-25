#!/usr/bin/env bash
# Tags: zookeeper, no-encrypted-storage, no-replicated-database
# Tag zookeeper: `ON CLUSTER` queries use the distributed DDL queue.
# Tag no-encrypted-storage: a backup from an encrypted disk restores only to an encrypted disk, so the Backup database gets no parts.
# Tag no-replicated-database: the CREATE DATABASE runs with no user there, so its check is a no-op.

# A `Backup` database reads its backup and the base backups named in the backup's metadata with no user, so creating
# one, directly, `ON CLUSTER` or by `RESTORE` of its definition, requires the source grant of every base backup.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}"
user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
cluster="ON CLUSTER test_shard_localhost"
base="File('${CLICKHOUSE_TEST_UNIQUE_NAME}/base')"
outer="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}/outer')"
emb="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}/emb')"

# `outer` gets its own `.sql` (no comment), so only the data part, stored in `base` alone, is read from the base.
# `emb` holds only the definition of a `Backup` database over `outer`.
${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${user};
CREATE USER ${user} IDENTIFIED WITH no_password;
GRANT CREATE DATABASE ON *.* TO ${user};
GRANT TABLE ENGINE ON *.* TO ${user};
GRANT CLUSTER ON *.* TO ${user};
GRANT READ ON DISK TO ${user};
GRANT SELECT ON ${db}_d1.* TO ${user};
GRANT SELECT ON ${db}_d2.* TO ${user};
GRANT SELECT ON ${db}_d3.* TO ${user};
GRANT SELECT ON ${db}_d4.* TO ${user};
CREATE TABLE ${db}.t (x UInt64) ENGINE = MergeTree ORDER BY x COMMENT 'level 0';
INSERT INTO ${db}.t VALUES (7);
BACKUP TABLE ${db}.t TO ${base} FORMAT Null;
ALTER TABLE ${db}.t MODIFY COMMENT '';
BACKUP TABLE ${db}.t TO ${outer} SETTINGS base_backup = ${base} FORMAT Null;
CREATE DATABASE ${db}_emb ENGINE = Backup('${db}', ${outer});
BACKUP DATABASE ${db}_emb TO ${emb} FORMAT Null;
DROP DATABASE ${db}_emb;
"

# Prints the error code and the grant that the query of the user lacks, or the word given.
function run()
{
    local output
    output=$(${CLICKHOUSE_CLIENT} --user "${user}" --distributed_ddl_output_mode throw -q "$2" 2>&1)
    if echo "${output}" | grep -q -E 'Code: [0-9]+'; then
        echo "$(echo "${output}" | grep -o -m1 -E 'Code: [0-9]+') $(echo "${output}" | grep -o -m1 -E 'grant [A-Z ]+ ON [A-Za-z]+')"
    else
        echo "$1"
    fi
}

function create_all()
{
    run created "CREATE DATABASE ${db}_d1 ENGINE = Backup('${db}', ${outer})"
    run created "CREATE DATABASE ${db}_d2 ${cluster} ENGINE = Backup('${db}', ${outer})"
    run restored "RESTORE DATABASE ${db}_emb AS ${db}_d3 FROM ${emb} FORMAT Null"
    run restored "RESTORE DATABASE ${db}_emb AS ${db}_d4 ${cluster} FROM ${emb} FORMAT Null"
}

echo '--- without READ ON FILE, the base backup itself is refused'
run created "CREATE DATABASE ${db}_direct ENGINE = Backup('${db}', ${base})"

echo '--- without READ ON FILE, so is a backup whose base it is: CREATE, CREATE ON CLUSTER, RESTORE, RESTORE ON CLUSTER'
create_all
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM system.databases WHERE name IN ('${db}_d1', '${db}_d2', '${db}_d3', '${db}_d4')"

echo '--- with READ ON FILE, the row stored only in the base is read'
${CLICKHOUSE_CLIENT} -q "GRANT READ ON FILE TO ${user}"
create_all
for d in d1 d2 d3 d4; do
    ${CLICKHOUSE_CLIENT} --user "${user}" -q "SELECT x FROM ${db}_${d}.t"
done

${CLICKHOUSE_CLIENT} -q "
DROP DATABASE IF EXISTS ${db}_direct;
DROP DATABASE IF EXISTS ${db}_d1;
DROP DATABASE IF EXISTS ${db}_d2;
DROP DATABASE IF EXISTS ${db}_d3;
DROP DATABASE IF EXISTS ${db}_d4;
DROP TABLE ${db}.t;
DROP USER ${user};
"
