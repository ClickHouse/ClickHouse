#!/usr/bin/env bash
# Tags: no-encrypted-storage

set -e
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db=${CLICKHOUSE_DATABASE}_selection
flat="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_flat')"
clustered="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_clustered')"
escaped_flat="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_escaped_flat')"
escaped_clustered="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_escaped_clustered')"

$CLICKHOUSE_CLIENT -nm -q "
DROP DATABASE IF EXISTS ${db}_flat;
DROP DATABASE IF EXISTS ${db}_clustered;
DROP DATABASE IF EXISTS ${db}_explicit;
DROP DATABASE IF EXISTS ${db};
CREATE DATABASE ${db};
CREATE TABLE ${db}.events (id UInt64, marker String) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${db}.events VALUES (11, 'source');
BACKUP DATABASE ${db} TO ${flat} FORMAT Null;
CREATE DATABASE ${db}_flat ENGINE = Backup('${db}', ${flat});
SELECT * FROM ${db}_flat.events;
BACKUP DATABASE ${db} ON CLUSTER test_shard_localhost TO ${clustered} FORMAT Null;
CREATE DATABASE ${db}_clustered ENGINE = Backup('${db}', ${clustered});
SELECT * FROM ${db}_clustered.events;
CREATE DATABASE ${db}_explicit ENGINE = Backup('${db}', ${clustered}) SETTINGS shard_num_in_backup = 1, replica_num_in_backup = 1;
SELECT * FROM ${db}_explicit.events;
ALTER DATABASE ${db}_explicit MODIFY COMMENT 'selected';
DETACH DATABASE ${db}_explicit;
ATTACH DATABASE ${db}_explicit;
SELECT * FROM ${db}_explicit.events;
DROP DATABASE ${db}_explicit;
CREATE DATABASE ${db}_explicit ENGINE = Backup('${db}', ${clustered}) SETTINGS shard_num_in_backup = 99, shard_num_in_backup = '1', replica_num_in_backup = '1';
SELECT * FROM ${db}_explicit.events;
DROP DATABASE ${db}_explicit;
CREATE DATABASE ${db}_explicit ENGINE = Backup('${db}', ${flat}) SETTINGS shard_num_in_backup = 0, replica_num_in_backup = 1;
SELECT * FROM ${db}_explicit.events;
DROP DATABASE ${db}_explicit;
CREATE DATABASE ${db}_explicit ENGINE = Backup('${db}', ${flat}) SETTINGS shard_num_in_backup = 1.0;
SELECT * FROM ${db}_explicit.events;
DROP DATABASE ${db}_explicit;
DROP DATABASE IF EXISTS \`${db}-escaped\`;
CREATE DATABASE \`${db}-escaped\`;
CREATE TABLE \`${db}-escaped\`.events (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO \`${db}-escaped\`.events VALUES (42);
BACKUP DATABASE \`${db}-escaped\` TO ${escaped_flat} FORMAT Null;
CREATE DATABASE ${db}_explicit ENGINE = Backup('${db}-escaped', ${escaped_flat});
SELECT * FROM ${db}_explicit.events;
DROP DATABASE ${db}_explicit;
BACKUP DATABASE \`${db}-escaped\` ON CLUSTER test_shard_localhost TO ${escaped_clustered} FORMAT Null;
CREATE DATABASE ${db}_explicit ENGINE = Backup('${db}-escaped', ${escaped_clustered});
SELECT * FROM ${db}_explicit.events;
DROP DATABASE ${db}_explicit;
DROP DATABASE \`${db}-escaped\`;
"

expect_error() {
    local expected=$1 query=$2 output
    if output=$($CLICKHOUSE_CLIENT --query "$query" 2>&1); then
        echo "Expected ${expected}, but query succeeded" >&2
        exit 1
    fi
    if [[ $output != *"${expected}"* ]]; then
        echo "$output" >&2
        exit 1
    fi
    echo "$expected"
}

expect_error BACKUP_ENTRY_NOT_FOUND "CREATE DATABASE ${db}_explicit ENGINE = Backup('${db}', ${clustered}) SETTINGS shard_num_in_backup = 99"
expect_error BACKUP_ENTRY_NOT_FOUND "CREATE DATABASE ${db}_explicit ENGINE = Backup('${db}', ${clustered}) SETTINGS replica_num_in_backup = 99"
expect_error BAD_ARGUMENTS "CREATE DATABASE ${db}_explicit ENGINE = Backup('${db}', ${clustered}) SETTINGS shard_num_in_backup = 1.5"
expect_error CANNOT_CONVERT_TYPE "CREATE DATABASE ${db}_explicit ENGINE = Backup('${db}', ${clustered}) SETTINGS shard_num_in_backup = -1"
expect_error CANNOT_PARSE_NUMBER "CREATE DATABASE ${db}_explicit ENGINE = Backup('${db}', ${clustered}) SETTINGS shard_num_in_backup = '18446744073709551616'"
expect_error CANNOT_PARSE_NUMBER "CREATE DATABASE ${db}_explicit ENGINE = Backup('${db}', ${clustered}) SETTINGS shard_num_in_backup = '18446744073709552K'"
expect_error BACKUP_ENTRY_NOT_FOUND "CREATE DATABASE ${db}_explicit ENGINE = Backup('${db}', ${clustered}) SETTINGS shard_num_in_backup = '1K'"
expect_error UNKNOWN_SETTING "CREATE DATABASE ${db}_explicit ENGINE = Backup('${db}', ${clustered}) SETTINGS unknown_backup_selector = 1"
expect_error UNKNOWN_SETTING "CREATE DATABASE ${db}_explicit ENGINE = Backup('${db}', ${clustered}) SETTINGS unknown_backup_selector = DEFAULT"
expect_error BAD_ARGUMENTS "CREATE DATABASE ${db}_explicit ENGINE = Backup('${db}', ${clustered}) SETTINGS shard_num_in_backup = DEFAULT"
expect_error BAD_ARGUMENTS "CREATE DATABASE ${db}_explicit ENGINE = Backup('${db}', ${clustered}) SETTINGS param_selector = '1'"

$CLICKHOUSE_CLIENT -nm -q "DROP DATABASE ${db}_clustered; DROP DATABASE ${db}_flat; DROP DATABASE ${db};"
