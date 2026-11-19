#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Restoring a cluster-mode `QueryRunner` table requires the `REMOTE` grant, like creating it.

db=${CLICKHOUSE_DATABASE}
user="user_05838_${CLICKHOUSE_DATABASE}"
backup="Disk('backups', '05838_${CLICKHOUSE_TEST_UNIQUE_NAME}')"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS $user"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE $db.runner (query String) ENGINE = QueryRunner SETTINGS cluster = 'test_shard_localhost', mode = 'synchronous'"
${CLICKHOUSE_CLIENT} -q "BACKUP TABLE $db.runner TO $backup FORMAT Null"
${CLICKHOUSE_CLIENT} -q "DROP TABLE $db.runner SYNC"

${CLICKHOUSE_CLIENT} <<EOL
CREATE USER $user;
GRANT CREATE TABLE, SHOW TABLES, SHOW COLUMNS ON $db.* TO $user;
GRANT TABLE ENGINE ON QueryRunner TO $user;
GRANT CLUSTER ON *.* TO $user;
GRANT READ ON DISK TO $user;
EOL

echo "without the REMOTE grant"
${CLICKHOUSE_CLIENT} --user "$user" -q "RESTORE TABLE $db.runner FROM $backup FORMAT Null" 2>&1 | grep -o -m1 "the grant READ, WRITE ON REMOTE"
${CLICKHOUSE_CLIENT} --user "$user" -q "RESTORE TABLE $db.runner ON CLUSTER test_shard_localhost FROM $backup FORMAT Null" 2>&1 | grep -o -m1 "the grant READ, WRITE ON REMOTE"
${CLICKHOUSE_CLIENT} -q "EXISTS TABLE $db.runner"

echo "with the REMOTE grant"
${CLICKHOUSE_CLIENT} -q "GRANT READ, WRITE ON REMOTE TO $user"
${CLICKHOUSE_CLIENT} --user "$user" -q "RESTORE TABLE $db.runner FROM $backup FORMAT Null"
${CLICKHOUSE_CLIENT} -q "EXISTS TABLE $db.runner"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS $db.runner SYNC"
${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS $user"
