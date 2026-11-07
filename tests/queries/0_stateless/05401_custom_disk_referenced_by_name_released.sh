#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree
# no-shared-merge-tree: custom disk

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="${CLICKHOUSE_CLIENT} --database_atomic_wait_for_drop_and_detach_synchronously 1 --multiline"

# A disk defined inline with `disk(...)` stays registered while a table reaches it by name: through its
# implicit storage policy `__<disk>`, or as the disk wrapped by a `cache` disk.
disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_disk"
cache_disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_cache_disk"
wrapper_disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_wrapper_disk"
inner_disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_inner_disk"
other_inner_disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_other_inner_disk"

$CLIENT -q "
DROP TABLE IF EXISTS test_defining SYNC;
DROP TABLE IF EXISTS test_policy SYNC;
DROP TABLE IF EXISTS test_cache SYNC;

CREATE TABLE test_defining (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$disk', type = 'local_blob_storage', path = '${CLICKHOUSE_DISKS_FILES}/$disk/');

CREATE TABLE test_policy (a Int32) ENGINE = MergeTree() ORDER BY tuple() SETTINGS storage_policy = '__$disk';

CREATE TABLE test_cache (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$cache_disk', type = cache, max_size = '1Mi', max_file_segment_size = '256Ki', boundary_alignment = '256Ki', path = '$cache_disk/', disk = '$disk');

DROP TABLE test_defining SYNC;
SELECT 'disks after dropping the defining table', count() FROM system.disks WHERE name IN ('$disk', '$cache_disk');

INSERT INTO test_policy SELECT number FROM numbers(5);
SELECT 'rows in the table using the implicit storage policy', count() FROM test_policy;
INSERT INTO test_cache SELECT number FROM numbers(6);
SELECT 'rows in the table using the cache disk', count() FROM test_cache;

DROP TABLE test_cache SYNC;
SELECT 'disks after dropping the cache table', count() FROM system.disks WHERE name IN ('$disk', '$cache_disk');
DROP TABLE test_policy SYNC;
SELECT 'disks after dropping all tables', count() FROM system.disks WHERE name IN ('$disk', '$cache_disk');
"

# A wrapper with the name of an existing one but another nested disk is rejected, and the new nested disk is released.
$CLIENT -q "
DROP TABLE IF EXISTS test_wrapper SYNC;
DROP TABLE IF EXISTS test_other_wrapper SYNC;

CREATE TABLE test_wrapper (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$wrapper_disk', type = cache, max_size = '1Mi', max_file_segment_size = '256Ki', boundary_alignment = '256Ki', path = '$wrapper_disk/',
                     disk = disk(name = '$inner_disk', type = 'local_blob_storage', path = '${CLICKHOUSE_DISKS_FILES}/$inner_disk/'));

CREATE TABLE test_other_wrapper (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$wrapper_disk', type = cache, max_size = '1Mi', max_file_segment_size = '256Ki', boundary_alignment = '256Ki', path = '$wrapper_disk/',
                     disk = disk(name = '$other_inner_disk', type = 'local_blob_storage', path = '${CLICKHOUSE_DISKS_FILES}/$other_inner_disk/')); -- { serverError BAD_ARGUMENTS }

SELECT 'disks after the rejected wrapper', count() FROM system.disks WHERE name IN ('$wrapper_disk', '$inner_disk', '$other_inner_disk');
INSERT INTO test_wrapper SELECT number FROM numbers(7);
SELECT 'rows in the wrapper table', count() FROM test_wrapper;

DROP TABLE test_wrapper SYNC;
SELECT 'disks after drop', count() FROM system.disks WHERE name IN ('$wrapper_disk', '$inner_disk', '$other_inner_disk');
"
