#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree
# no-shared-merge-tree: custom disk

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="${CLICKHOUSE_CLIENT} --database_atomic_wait_for_drop_and_detach_synchronously 1 --multiline"

# The name of a disk defined inline is taken from the resolved configuration of the disk, so a numeric `name`
# argument gives a disk whose name is the number. Another table may refer to it by that name with the implicit
# storage policy of the disk. When the database is attached, the table defining that disk must be scanned before
# the table referring to it, regardless of the order of the names of the metadata files: `z_cache` defines the
# `cache` disk with a numeric name wrapping the disk of `z_base`, and `a_policy` uses the implicit storage policy
# of the `cache` disk.
db="${CLICKHOUSE_DATABASE}_reattach"
base_disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_base_disk"
cache_disk=$($CLIENT -q "SELECT cityHash64('$CLICKHOUSE_TEST_UNIQUE_NAME')")
cache_path="${CLICKHOUSE_TEST_UNIQUE_NAME}_numeric_cache/"
disks="'$base_disk', '$cache_disk'"

$CLIENT -q "
DROP DATABASE IF EXISTS $db SYNC;
CREATE DATABASE $db ENGINE = Atomic;

CREATE TABLE $db.z_base (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$base_disk', type = 'local_blob_storage', path = '${CLICKHOUSE_DISKS_FILES}/$base_disk/');

CREATE TABLE $db.z_cache (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = $cache_disk, type = cache, disk = '$base_disk', path = '$cache_path', max_size = '1Mi', max_file_segment_size = '256Ki', boundary_alignment = '256Ki');

SELECT 'numeric name', count() FROM system.disks WHERE name = '$cache_disk';

CREATE TABLE $db.a_policy (a Int32) ENGINE = MergeTree() ORDER BY tuple() SETTINGS storage_policy = '__$cache_disk';

INSERT INTO $db.z_base SELECT number FROM numbers(5);
INSERT INTO $db.z_cache SELECT number FROM numbers(6);
INSERT INTO $db.a_policy SELECT number FROM numbers(7);

DETACH DATABASE $db;
SELECT 'disks after detach', count() FROM system.disks WHERE name IN ($disks);

ATTACH DATABASE $db;
SELECT 'rows in the base table', count() FROM $db.z_base;
SELECT 'rows in the cache table', count() FROM $db.z_cache;
SELECT 'rows in the table on the implicit policy', count() FROM $db.a_policy;
SELECT 'disks after attach', count() FROM system.disks WHERE name IN ($disks);

DROP DATABASE $db SYNC;
SELECT 'disks after drop', count() FROM system.disks WHERE name IN ($disks);
"
