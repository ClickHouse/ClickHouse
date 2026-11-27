#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings
# no-fasttest: needs S3
# no-random-settings, no-random-merge-tree-settings: the test compares memory, which the randomized buffer and block
# sizes change by more than the difference it checks

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A read of 100 small regions of SHARED paths separately, through the filesystem cache, sizes the buffer of every region
# stream by its region, as a read of typed paths sizes the buffers of their files by the files: the cache's floor for
# buffers is capped by the size of the file read, which is the whole region file for a view.

decl_typed=""
decl_shared=""
for k in $(seq 0 99); do
    decl_typed+="${decl_typed:+, }g.k${k} UInt32"
    decl_shared+="${decl_shared:+, }SHARED g.k${k} UInt32"
done

for layout in typed v2; do
    if [ "$layout" = typed ]; then decl=$decl_typed; version=""; else decl=$decl_shared; version="object_typed_shared_data_serialization_version = 'v2',"; fi
    cache_name="${CLICKHOUSE_TEST_UNIQUE_NAME}_${layout}"
    $CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS t_view_buffers_${layout};
    CREATE TABLE t_view_buffers_${layout} (x UInt64, json JSON(${decl}))
    ENGINE = MergeTree ORDER BY x
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ${version}
             disk = disk(type = cache, max_size = '1Gi', path = '${cache_name}', name = '${cache_name}', cache_on_write_operations = 0, disk = 's3_disk');
    INSERT INTO t_view_buffers_${layout}
    SELECT number, concat('{\"g\":{', arrayStringConcat(arrayMap(k -> concat('\"k', toString(k), '\":', toString((number + k) % 1000)), range(100)), ','), '}}')
    FROM numbers(20000);

    SYSTEM DROP FILESYSTEM CACHE '${cache_name}';
    SELECT sum(cityHash64(x, toString(json.^g))) FROM t_view_buffers_${layout} FORMAT Null
    SETTINGS max_threads = 1, enable_filesystem_cache = 1, filesystem_cache_prefer_bigger_buffer_size = 1, prefetch_buffer_size = 1048576,
             remote_filesystem_read_method = 'threadpool', remote_filesystem_read_prefetch = 1, max_read_buffer_size = 1048576,
             object_shared_data_max_sub_object_paths_to_read_separately = 128, enable_reads_from_columns_cache = 0,
             enable_parallel_replicas = 0, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_${layout}';
    "
done

$CLICKHOUSE_CLIENT -m -q "
SYSTEM FLUSH LOGS query_log;
SELECT
    (SELECT max(memory_usage) FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_v2')
    < (SELECT max(memory_usage) FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_typed') + 24 * 1024 * 1024;
DROP TABLE t_view_buffers_typed;
DROP TABLE t_view_buffers_v2;
"
