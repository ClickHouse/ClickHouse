#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-object-storage, no-replicated-database, no-shared-merge-tree
# no-parallel: the fail point is global, and would fail the creation of the custom disks of other tests.
# no-fasttest: tests that enable a fail point run alone, and are kept out of the fast test.
# no-shared-merge-tree: custom disk

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The registration of a disk defined inline with `disk(...)` owns the registrations of the disks nested in its
# definition. If it cannot be allocated, the nested registrations have no other owner, and they must not be destroyed
# while the locks of the disks are held: destroying one releases its disk, which takes the same locks. The query
# has to fail, leave no disk behind, and the same definition has to work afterwards.

CLIENT="${CLICKHOUSE_CLIENT} --database_atomic_wait_for_drop_and_detach_synchronously 1"

inner_disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_inner_disk"
cache_disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_cache_disk"

trap '$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT custom_disk_registry_fail_to_allocate_wrapper_registration"' EXIT

create="CREATE TABLE test_cache_disk (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$cache_disk', type = cache, max_size = '1Mi', max_file_segment_size = '256Ki', boundary_alignment = '256Ki', path = '$cache_disk/',
                     disk = disk(name = '$inner_disk', type = 'local_blob_storage', path = '${CLICKHOUSE_DISKS_FILES}/$inner_disk/'))"

$CLIENT -q "
DROP TABLE IF EXISTS test_cache_disk SYNC;

SYSTEM ENABLE FAILPOINT custom_disk_registry_fail_to_allocate_wrapper_registration;
$create; -- { serverError FAULT_INJECTED }
SELECT 'disks after the failed create', count() FROM system.disks WHERE name IN ('$inner_disk', '$cache_disk');

$create;
INSERT INTO test_cache_disk SELECT number FROM numbers(5);
SELECT 'rows', count() FROM test_cache_disk;
SELECT 'disks after create', count() FROM system.disks WHERE name IN ('$inner_disk', '$cache_disk');

DROP TABLE test_cache_disk SYNC;
SELECT 'disks after drop', count() FROM system.disks WHERE name IN ('$inner_disk', '$cache_disk');
"
