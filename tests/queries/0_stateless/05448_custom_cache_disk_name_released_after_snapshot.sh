#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree
# no-shared-merge-tree: custom disk

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="${CLICKHOUSE_CLIENT} --database_atomic_wait_for_drop_and_detach_synchronously 1 --multiline"

# The filesystem cache of a `cache` disk defined inline with `disk(...)` is released together with the disk. If a query
# reading `system.filesystem_cache` still uses the cache at that moment, the cache stays until the query is finished,
# and then its name has to be released as well, so that it can be used again by a cache disk with another path.
cache_disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_cache_disk"
inner_disk="${CLICKHOUSE_TEST_UNIQUE_NAME}_inner_disk"
query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_snapshot"

$CLIENT -q "
DROP TABLE IF EXISTS test_cache SYNC;

CREATE TABLE test_cache (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$cache_disk', type = cache, max_size = '1Mi', max_file_segment_size = '256Ki', boundary_alignment = '256Ki', path = '${cache_disk}_first/',
                     disk = disk(name = '$inner_disk', type = 'local_blob_storage', path = '${CLICKHOUSE_DISKS_FILES}/$inner_disk/'));
INSERT INTO test_cache SELECT number FROM numbers(5);
SELECT 'rows with the first cache path', count() FROM test_cache;
"

$CLIENT --query_id "$query_id" -q "SELECT sleep(3), count() > -1 FROM system.filesystem_cache FORMAT Null" &

for _ in {1..300}; do
    [ "$($CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$query_id'")" = 1 ] && break
    sleep 0.1
done

$CLIENT -q "DROP TABLE test_cache SYNC"

wait

# The name is removed by the first access to the filesystem caches after the query is finished. The disk itself is
# shut down in the background, so it is waited for.
for _ in {1..600}; do
    [ "$($CLIENT -q "SELECT count() FROM system.filesystem_cache_settings WHERE cache_name = '$cache_disk'")" = 0 ] && break
    sleep 0.1
done
$CLIENT -q "SELECT 'caches with the name after the query', count() FROM system.filesystem_cache_settings WHERE cache_name = '$cache_disk'"

$CLIENT -q "
CREATE TABLE test_cache (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = '$cache_disk', type = cache, max_size = '1Mi', max_file_segment_size = '256Ki', boundary_alignment = '256Ki', path = '${cache_disk}_second/',
                     disk = disk(name = '$inner_disk', type = 'local_blob_storage', path = '${CLICKHOUSE_DISKS_FILES}/$inner_disk/'));
INSERT INTO test_cache SELECT number FROM numbers(6);
SELECT 'rows with the second cache path', count() FROM test_cache;
SELECT 'cache path', path LIKE '%${cache_disk}_second/' FROM system.filesystem_cache_settings WHERE cache_name = '$cache_disk';
DROP TABLE test_cache SYNC;
"
