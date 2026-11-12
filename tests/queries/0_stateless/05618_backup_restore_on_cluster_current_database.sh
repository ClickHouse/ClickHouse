#!/usr/bin/env bash
# An unqualified table name in BACKUP/RESTORE ON CLUSTER means the current database, as it does locally.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

b="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}"
user="u_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
CREATE TABLE bk_src (x Int32) ENGINE = MergeTree ORDER BY x;
INSERT INTO bk_src VALUES (1), (2);
"
$CLICKHOUSE_CLIENT -q "BACKUP TABLE bk_src ON CLUSTER test_shard_localhost TO ${b}_oc')" | grep -o BACKUP_CREATED
$CLICKHOUSE_CLIENT -q "RESTORE TABLE bk_src AS bk_local FROM ${b}_oc')" | grep -o RESTORED
$CLICKHOUSE_CLIENT -q "RESTORE TABLE bk_src AS bk_cluster ON CLUSTER test_shard_localhost FROM ${b}_oc')" | grep -o RESTORED
$CLICKHOUSE_CLIENT -q "SELECT groupArray(x) FROM (SELECT x FROM bk_local ORDER BY x)"
$CLICKHOUSE_CLIENT -q "SELECT groupArray(x) FROM (SELECT x FROM bk_cluster ORDER BY x)"

$CLICKHOUSE_CLIENT -m -q "
DROP USER IF EXISTS $user;
CREATE USER $user;
GRANT BACKUP ON ${CLICKHOUSE_DATABASE}.bk_src TO $user;
GRANT CLUSTER ON *.* TO $user;
GRANT WRITE ON DISK TO $user;
"
$CLICKHOUSE_CLIENT --user $user -q "BACKUP TABLE bk_src ON CLUSTER test_shard_localhost TO ${b}_user')" | grep -o BACKUP_CREATED
$CLICKHOUSE_CLIENT -q "DROP USER $user"
