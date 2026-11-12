#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree
# no-shared-merge-tree: custom disk

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="${CLICKHOUSE_CLIENT} --database_atomic_wait_for_drop_and_detach_synchronously 1 --multiline"

# When a custom disk defined inline with `disk(...)` is released, the filesystem cache registered under its name is
# released too, but only if the custom disk registered it. A filesystem cache from the configuration with the same
# name as an unrelated custom disk (here, `cache_for_query_results` from the test configuration) stays.
# The definition of the disk does not depend on the test, so the concurrent runs of the test share the disk.
$CLIENT -q "
SELECT 'configured cache before', count() FROM system.filesystem_cache_settings WHERE cache_name = 'cache_for_query_results';

DROP TABLE IF EXISTS test_disk SYNC;
CREATE TABLE test_disk (a Int32) ENGINE = MergeTree() ORDER BY tuple()
SETTINGS disk = disk(name = 'cache_for_query_results', type = 'local_blob_storage', path = '${CLICKHOUSE_DISKS_FILES}/cache_for_query_results_05443/');
INSERT INTO test_disk SELECT number FROM numbers(5);
SELECT 'rows', count() FROM test_disk;
DROP TABLE test_disk SYNC;

SELECT 'configured cache after', count() FROM system.filesystem_cache_settings WHERE cache_name = 'cache_for_query_results';
"
