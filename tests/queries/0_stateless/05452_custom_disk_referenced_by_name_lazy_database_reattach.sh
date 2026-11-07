#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree
# no-shared-merge-tree: custom disk

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="${CLICKHOUSE_CLIENT} --database_atomic_wait_for_drop_and_detach_synchronously 1 --multiline"

# With `lazy_load_tables`, the disks defined inline with `disk(...)` are not created by the metadata scan. A table
# referring by name to such a disk (with the implicit storage policy of the disk) must not make `ATTACH DATABASE` fail
# after the disk was released with the database. The tables are loaded on first access.
db="${CLICKHOUSE_DATABASE}_lazy_reattach"
disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_disk"

$CLIENT -q "
DROP DATABASE IF EXISTS $db SYNC;
CREATE DATABASE $db ENGINE = Atomic SETTINGS lazy_load_tables = 1;

CREATE TABLE $db.z_defining (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$disk', type = 'local_blob_storage', path = '${CLICKHOUSE_DISKS_FILES}/$disk/');

CREATE TABLE $db.a_policy (a Int32) ENGINE = MergeTree() ORDER BY tuple() SETTINGS storage_policy = '__$disk';

INSERT INTO $db.z_defining SELECT number FROM numbers(5);
INSERT INTO $db.a_policy SELECT number FROM numbers(6);

DETACH DATABASE $db;
SELECT 'disks after detach', count() FROM system.disks WHERE name = '$disk';

ATTACH DATABASE $db;
SELECT 'rows in the defining table', count() FROM $db.z_defining;
SELECT 'rows in the table on the implicit policy', count() FROM $db.a_policy;

DROP DATABASE $db SYNC;
SELECT 'disks after drop', count() FROM system.disks WHERE name = '$disk';
"
