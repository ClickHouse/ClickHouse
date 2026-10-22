#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree
# no-shared-merge-tree: custom disk

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="${CLICKHOUSE_CLIENT} --database_atomic_wait_for_drop_and_detach_synchronously 1 --multiline"

# When a database is attached, its metadata is scanned before the tables are loaded, and the scan creates
# the disks defined inline with `disk(...)`. A table may refer by name to a disk defined by another table
# (here, as the disk wrapped by a `cache` disk), so the disks created by the scan must stay registered until
# the tables are loaded, and the defining table must be scanned first regardless of the order of the names of
# the metadata files, which are scanned in parallel. Both orders of the names are checked.
db="${CLICKHOUSE_DATABASE}_reattach"
inner_disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_inner_disk"
cache_disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_cache_disk"
inner_disk2="${CLICKHOUSE_TEST_UNIQUE_NAME}_inner_disk2"
cache_disk2="${CLICKHOUSE_TEST_UNIQUE_NAME}_cache_disk2"
disks="'$inner_disk', '$cache_disk', '$inner_disk2', '$cache_disk2'"

$CLIENT -q "
DROP DATABASE IF EXISTS $db SYNC;
CREATE DATABASE $db ENGINE = Atomic;

CREATE TABLE $db.a_defining (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$inner_disk', type = 'local_blob_storage', path = '${CLICKHOUSE_DISKS_FILES}/$inner_disk/');

CREATE TABLE $db.b_cache (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$cache_disk', type = cache, max_size = '1Mi', path = '$cache_disk/', disk = '$inner_disk');

CREATE TABLE $db.z_defining (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$inner_disk2', type = 'local_blob_storage', path = '${CLICKHOUSE_DISKS_FILES}/$inner_disk2/');

CREATE TABLE $db.a_cache (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$cache_disk2', type = cache, max_size = '1Mi', path = '$cache_disk2/', disk = '$inner_disk2');

INSERT INTO $db.a_defining SELECT number FROM numbers(5);
INSERT INTO $db.b_cache SELECT number FROM numbers(6);
INSERT INTO $db.z_defining SELECT number FROM numbers(7);
INSERT INTO $db.a_cache SELECT number FROM numbers(8);

DETACH DATABASE $db;
SELECT 'disks after detach', count() FROM system.disks WHERE name IN ($disks);

ATTACH DATABASE $db;
SELECT 'rows in the defining table', count() FROM $db.a_defining;
SELECT 'rows in the cache table', count() FROM $db.b_cache;
SELECT 'rows in the defining table with the last name', count() FROM $db.z_defining;
SELECT 'rows in the cache table with the first name', count() FROM $db.a_cache;
SELECT 'disks after attach', count() FROM system.disks WHERE name IN ($disks);

DROP DATABASE $db SYNC;
SELECT 'disks after drop', count() FROM system.disks WHERE name IN ($disks);
"
