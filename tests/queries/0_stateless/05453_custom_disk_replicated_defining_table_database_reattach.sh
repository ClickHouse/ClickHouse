#!/usr/bin/env bash
# Tags: zookeeper, no-object-storage, no-replicated-database, no-shared-merge-tree
# no-shared-merge-tree: custom disk

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="${CLICKHOUSE_CLIENT} --database_atomic_wait_for_drop_and_detach_synchronously 1 --multiline"

# When a database is attached, its metadata scan creates the disks defined inline with `disk(...)`, so that the
# tables referring to them by name can be loaded. This is also done for a disk defined by a `ReplicatedMergeTree`
# table, which is not looked at otherwise because it is not converted to a replicated table.
db="${CLICKHOUSE_DATABASE}_reattach"
disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_disk"

$CLIENT -q "
DROP DATABASE IF EXISTS $db SYNC;
CREATE DATABASE $db ENGINE = Atomic;

CREATE TABLE $db.z_defining (a Int32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/z_defining', '1') ORDER BY tuple()
SETTINGS disk = disk(name = '$disk', type = 'local_blob_storage', path = '${CLICKHOUSE_DISKS_FILES}/$disk/');

CREATE TABLE $db.a_policy (a Int32) ENGINE = MergeTree() ORDER BY tuple() SETTINGS storage_policy = '__$disk';

INSERT INTO $db.z_defining SELECT number FROM numbers(5);
INSERT INTO $db.a_policy SELECT number FROM numbers(6);

DETACH DATABASE $db;
SELECT 'disks after detach', count() FROM system.disks WHERE name = '$disk';

ATTACH DATABASE $db;
SELECT 'rows in the replicated defining table', count() FROM $db.z_defining;
SELECT 'rows in the table on the implicit policy', count() FROM $db.a_policy;
SELECT 'disks after attach', count() FROM system.disks WHERE name = '$disk';

DROP DATABASE $db SYNC;
SELECT 'disks after drop', count() FROM system.disks WHERE name = '$disk';
"
