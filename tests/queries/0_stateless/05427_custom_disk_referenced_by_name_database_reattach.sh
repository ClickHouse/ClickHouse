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
# the tables are loaded. The metadata files are scanned in the order of their names: the defining table first.
db="${CLICKHOUSE_DATABASE}_reattach"
inner_disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_inner_disk"
cache_disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_cache_disk"

$CLIENT -q "
DROP DATABASE IF EXISTS $db SYNC;
CREATE DATABASE $db ENGINE = Atomic;

CREATE TABLE $db.a_defining (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$inner_disk', type = 'local_blob_storage', path = '${CLICKHOUSE_DISKS_FILES}/$inner_disk/');

CREATE TABLE $db.b_cache (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$cache_disk', type = cache, max_size = '1Mi', path = '$cache_disk/', disk = '$inner_disk');

INSERT INTO $db.a_defining SELECT number FROM numbers(5);
INSERT INTO $db.b_cache SELECT number FROM numbers(6);

DETACH DATABASE $db;
SELECT 'disks after detach', count() FROM system.disks WHERE name IN ('$inner_disk', '$cache_disk');

ATTACH DATABASE $db;
SELECT 'rows in the defining table', count() FROM $db.a_defining;
SELECT 'rows in the cache table', count() FROM $db.b_cache;
SELECT 'disks after attach', count() FROM system.disks WHERE name IN ('$inner_disk', '$cache_disk');

DROP DATABASE $db SYNC;
SELECT 'disks after drop', count() FROM system.disks WHERE name IN ('$inner_disk', '$cache_disk');
"
