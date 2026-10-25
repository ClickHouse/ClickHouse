#!/usr/bin/env bash
# Tags: no-replicated-database
# Restoring a metadata version higher than the one in ZooKeeper into a read-only replica is rejected
# before /metadata or the replication log of the table are changed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BACKUP_NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}"
ZK_PATH="/clickhouse/tables/${CLICKHOUSE_DATABASE}/t"

create_tables() {
    $CLICKHOUSE_CLIENT -q "CREATE TABLE t_r1 (id UInt64, x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t', 'r1') ORDER BY id"
    $CLICKHOUSE_CLIENT -q "CREATE TABLE t_r2 (id UInt64, x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t', 'r2') ORDER BY id"
}

print_zookeeper_state() {
    $CLICKHOUSE_CLIENT -q "SELECT version FROM system.zookeeper WHERE path = '$ZK_PATH' AND name = 'metadata'"
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.zookeeper WHERE path = '$ZK_PATH/log'"
}

create_tables
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_r1 ADD COLUMN tmp UInt8 SETTINGS alter_sync = 2"
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_r1 DROP COLUMN tmp SETTINGS alter_sync = 2"
$CLICKHOUSE_CLIENT -q "BACKUP TABLE t_r2 TO Disk('backups', '$BACKUP_NAME') SETTINGS structure_only = 1 FORMAT Null"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_r1 SYNC"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_r2 SYNC"

create_tables
$CLICKHOUSE_CLIENT -q "DETACH TABLE t_r2"
$CLICKHOUSE_CLIENT -q "SYSTEM DROP REPLICA 'r2' FROM TABLE t_r1"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE t_r2"
$CLICKHOUSE_CLIENT -q "SELECT is_readonly FROM system.replicas WHERE database = currentDatabase() AND table = 't_r2'"

echo '--- before restore ---'
print_zookeeper_state
$CLICKHOUSE_CLIENT -q "RESTORE TABLE t_r2 FROM Disk('backups', '$BACKUP_NAME') SETTINGS structure_only = 1 FORMAT Null" 2>&1 | grep -o -m1 'TABLE_IS_READ_ONLY'
echo '--- after restore ---'
print_zookeeper_state

$CLICKHOUSE_CLIENT -q "DROP TABLE t_r1 SYNC"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_r2 SYNC"
