#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-object-storage, no-replicated-database, no-shared-merge-tree
# no-parallel: the fail point is global, and would pause the shutdown of the custom disks of other tests.
# no-fasttest: tests that enable a fail point run alone, and are kept out of the fast test.
# no-replicated-database: plain rewritable should not be shared between replicas.
# no-shared-merge-tree: custom disk

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A writable `plain_rewritable` disk defined inline with `disk(...)` is unregistered when the last table using it is
# detached, and then shut down in the background. Until then, a new writable disk over the same object storage prefix
# waits for it, but a read-only disk cannot overwrite its metadata, so it is created right away, as when the writer is
# still registered.

CLIENT="${CLICKHOUSE_CLIENT} --database_atomic_wait_for_drop_and_detach_synchronously 1"

disk_path="${CLICKHOUSE_DISKS_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}/"
writer_disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_writer"
reader_disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_reader"

trap '$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT custom_disk_registry_pause_before_shutdown"' EXIT

$CLIENT -q "
DROP TABLE IF EXISTS writer SYNC;
DROP TABLE IF EXISTS reader SYNC;

CREATE TABLE writer (a Int32) ENGINE = MergeTree ORDER BY a
SETTINGS table_disk = true,
    disk = disk(name = '$writer_disk', type = object_storage, object_storage_type = local, metadata_type = plain_rewritable, path = '$disk_path');
INSERT INTO writer SELECT number FROM numbers(3);
"

$CLIENT -q "SYSTEM ENABLE FAILPOINT custom_disk_registry_pause_before_shutdown"
$CLIENT -q "DETACH TABLE writer SYNC"
$CLIENT -q "SYSTEM WAIT FAILPOINT custom_disk_registry_pause_before_shutdown PAUSE"

echo "-- the writer disk is unregistered, but not shut down yet"
$CLIENT -q "SELECT count() FROM system.disks WHERE name = '$writer_disk'"

timeout 60 $CLIENT -q "
CREATE TABLE reader (a Int32) ENGINE = MergeTree ORDER BY a
SETTINGS table_disk = true,
    disk = disk(read_only = true, name = '$reader_disk', type = object_storage, object_storage_type = local, metadata_type = plain_rewritable, path = '$disk_path');
"
$CLIENT -q "SELECT 'reader', count() FROM system.tables WHERE database = currentDatabase() AND name = 'reader'"

$CLIENT -q "SYSTEM DISABLE FAILPOINT custom_disk_registry_pause_before_shutdown"

echo "-- after the shutdown"
$CLIENT -q "
ATTACH TABLE writer;
SELECT 'writer', count() FROM writer;
DROP TABLE reader SYNC;
DROP TABLE writer SYNC;
"
