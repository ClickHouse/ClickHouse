#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree
# no-shared-merge-tree: custom disk

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="${CLICKHOUSE_CLIENT} --database_atomic_wait_for_drop_and_detach_synchronously 1 --multiline"

# The filesystem cache of a `cache` disk defined inline with `disk(...)` is registered under the name of the disk.
# When the last table using the disk is gone, the cache is released together with the disk, so the name can be
# used again by a cache disk with another path.
cache_disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_cache_disk"
inner_disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_inner_disk"

$CLIENT -q "
DROP TABLE IF EXISTS test_cache SYNC;

CREATE TABLE test_cache (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$cache_disk', type = cache, max_size = '1Mi', max_file_segment_size = '256Ki', boundary_alignment = '256Ki', path = '${cache_disk}_first/',
                     disk = disk(name = '$inner_disk', type = 'local_blob_storage', path = '${CLICKHOUSE_DISKS_FILES}/$inner_disk/'));
INSERT INTO test_cache SELECT number FROM numbers(5);
SELECT 'rows with the first cache path', count() FROM test_cache;
DROP TABLE test_cache SYNC;
"

# A query of another test reading `system.filesystem_cache` at the same time may still use the cache, and then
# its name is removed only after that query is finished.
for _ in {1..600}; do
    [ "$($CLIENT -q "SELECT count() FROM system.filesystem_cache_settings WHERE cache_name = '$cache_disk'")" = 0 ] && break
    sleep 0.1
done

$CLIENT -q "
CREATE TABLE test_cache (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$cache_disk', type = cache, max_size = '1Mi', max_file_segment_size = '256Ki', boundary_alignment = '256Ki', path = '${cache_disk}_second/',
                     disk = disk(name = '$inner_disk', type = 'local_blob_storage', path = '${CLICKHOUSE_DISKS_FILES}/$inner_disk/'));
INSERT INTO test_cache SELECT number FROM numbers(6);
SELECT 'rows with the second cache path', count() FROM test_cache;
SELECT 'cache path', path LIKE '%${cache_disk}_second/' FROM system.filesystem_cache_settings WHERE cache_name = '$cache_disk';
DROP TABLE test_cache SYNC;
"
