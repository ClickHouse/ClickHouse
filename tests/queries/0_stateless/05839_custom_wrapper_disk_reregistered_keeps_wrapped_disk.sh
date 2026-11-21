#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, no-parallel, no-object-storage, no-replicated-database, no-shared-merge-tree
# zookeeper: the name of the wrapped disk is read with `from_zk`.
# no-parallel: the fail point is global, and would pause the release of the custom disks of other tests.
# no-fasttest: tests that enable a fail point run alone, and are kept out of the fast test.
# no-shared-merge-tree: custom disk

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `cache` disk names the disk it wraps with `from_zk`, and keeps that disk registered. While the last registration
# of the `cache` disk is being destroyed, the node changes and a table with the same definition registers the disk
# again. The disk is the same one and still wraps the old disk, so the new registration has to keep the old disk
# registered, not the one the definition names now.

CLIENT="${CLICKHOUSE_CLIENT} --database_atomic_wait_for_drop_and_detach_synchronously 1 --dynamic_disk_allow_from_zk 1"

old_disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_old"
new_disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_new"
cache_disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_cache"
zk_path="/clickhouse/${CLICKHOUSE_TEST_UNIQUE_NAME}"

cache_definition="disk(name = '$cache_disk', type = cache, max_size = '1Mi', max_file_segment_size = '256Ki', boundary_alignment = '256Ki', path = '$cache_disk/', disk = 'from_zk $zk_path')"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT custom_disk_registration_pause_before_release"
    $CLICKHOUSE_KEEPER_CLIENT -q "rmr '$zk_path'" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_KEEPER_CLIENT -q "create '$zk_path' '$old_disk'"

$CLIENT -q "
DROP TABLE IF EXISTS test_old SYNC;
DROP TABLE IF EXISTS test_new SYNC;
DROP TABLE IF EXISTS test_cache SYNC;
DROP TABLE IF EXISTS test_cache_again SYNC;
DROP TABLE IF EXISTS test_policy SYNC;

CREATE TABLE test_old (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$old_disk', type = 'local_blob_storage', path = '${CLICKHOUSE_DISKS_FILES}/$old_disk/');
CREATE TABLE test_new (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$new_disk', type = 'local_blob_storage', path = '${CLICKHOUSE_DISKS_FILES}/$new_disk/');
CREATE TABLE test_cache (a Int32) ENGINE = MergeTree() ORDER BY tuple() SETTINGS disk = $cache_definition;

-- The cache disk is the only one that keeps the disk it wraps registered.
DROP TABLE test_old SYNC;
"

echo "-- the wrapped disk is kept by the cache disk"
$CLIENT -q "SELECT count() FROM system.disks WHERE name IN ('$old_disk', '$new_disk', '$cache_disk')"

$CLIENT -q "SYSTEM ENABLE FAILPOINT custom_disk_registration_pause_before_release"
$CLIENT -q "DROP TABLE test_cache SYNC" &
$CLIENT -q "SYSTEM WAIT FAILPOINT custom_disk_registration_pause_before_release PAUSE"

$CLICKHOUSE_KEEPER_CLIENT -q "set '$zk_path' '$new_disk'"

$CLIENT -q "
CREATE TABLE test_cache_again (a Int32) ENGINE = MergeTree() ORDER BY tuple() SETTINGS disk = $cache_definition;
INSERT INTO test_cache_again VALUES (1);
"

$CLIENT -q "SYSTEM DISABLE FAILPOINT custom_disk_registration_pause_before_release"
wait

echo "-- the cache disk is registered again, and still keeps the disk it wraps"
$CLIENT -q "SELECT count() FROM system.disks WHERE name IN ('$old_disk', '$new_disk', '$cache_disk')"
$CLIENT -q "
CREATE TABLE test_policy (a Int32) ENGINE = MergeTree() ORDER BY tuple() SETTINGS storage_policy = '__$old_disk';
SELECT * FROM test_cache_again;
DROP TABLE test_policy SYNC;
"

echo "-- the wrapped disk is released together with the cache disk"
$CLIENT -q "DROP TABLE test_cache_again SYNC"
$CLIENT -q "SELECT name = '$new_disk' FROM system.disks WHERE name IN ('$old_disk', '$new_disk', '$cache_disk')"

$CLIENT -q "DROP TABLE test_new SYNC"
$CLIENT -q "SELECT count() FROM system.disks WHERE name IN ('$old_disk', '$new_disk', '$cache_disk')"
