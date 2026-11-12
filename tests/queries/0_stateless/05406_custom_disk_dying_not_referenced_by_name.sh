#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-object-storage, no-replicated-database, no-shared-merge-tree
# no-parallel: the fail point is global, and would pause the release of the custom disks of other tests.
# no-fasttest: tests that enable a fail point run alone, and are kept out of the fast test.
# no-shared-merge-tree: custom disk

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A disk defined inline with `disk(...)` whose last registration is being destroyed is about to be unregistered.
# A table must not start using it by name meanwhile - through its implicit storage policy `__<disk>`, or as the
# disk wrapped by a `cache` disk - since it would be left with a disk that does not exist anymore.

CLIENT="${CLICKHOUSE_CLIENT} --database_atomic_wait_for_drop_and_detach_synchronously 1"

disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_disk"
cache_disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_cache_disk"

trap '$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT custom_disk_registration_pause_before_release"' EXIT

$CLIENT -q "
DROP TABLE IF EXISTS test_defining SYNC;
DROP TABLE IF EXISTS test_policy SYNC;
DROP TABLE IF EXISTS test_cache SYNC;

CREATE TABLE test_defining (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$disk', type = 'local_blob_storage', path = '${CLICKHOUSE_DISKS_FILES}/$disk/');
"

$CLIENT -q "SYSTEM ENABLE FAILPOINT custom_disk_registration_pause_before_release"
$CLIENT -q "DROP TABLE test_defining SYNC" &
$CLIENT -q "SYSTEM WAIT FAILPOINT custom_disk_registration_pause_before_release PAUSE"

echo "-- the disk is still registered, but is being released"
$CLIENT -q "SELECT count() FROM system.disks WHERE name = '$disk'"

$CLIENT -q "CREATE TABLE test_policy (a Int32) ENGINE = MergeTree() ORDER BY tuple() SETTINGS storage_policy = '__$disk'" 2>&1 \
    | grep -o -m1 'UNKNOWN_DISK'
$CLIENT -q "
CREATE TABLE test_cache (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$cache_disk', type = cache, max_size = '1Mi', max_file_segment_size = '256Ki', boundary_alignment = '256Ki', path = '$cache_disk/', disk = '$disk')" 2>&1 \
    | grep -o -m1 'UNKNOWN_DISK'

$CLIENT -q "SYSTEM DISABLE FAILPOINT custom_disk_registration_pause_before_release"
wait

echo "-- after the release"
$CLIENT -q "SELECT count() FROM system.disks WHERE name IN ('$disk', '$cache_disk')"
$CLIENT -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name IN ('test_policy', 'test_cache')"
