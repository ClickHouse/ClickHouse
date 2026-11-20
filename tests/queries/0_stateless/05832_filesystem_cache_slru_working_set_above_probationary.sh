#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: requires a cache disk.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

cache_name="05832_slru_working_set_${CLICKHOUSE_DATABASE}"
qid_prefix="05832_${CLICKHOUSE_DATABASE}"

# SLRU with a 6 MiB protected and a 4 MiB probationary queue.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t (key UInt64 CODEC(NONE), value UInt64 CODEC(NONE))
    ENGINE = MergeTree ORDER BY key
    SETTINGS disk = disk(
        type = cache,
        name = '$cache_name',
        path = '$cache_name',
        disk = 'local_disk',
        max_size = '10Mi',
        cache_policy = 'SLRU',
        slru_size_ratio = 0.6,
        max_file_segment_size = '1Mi',
        boundary_alignment = '1Mi',
        background_download_threads = 0,
        cache_on_write_operations = 0),
    min_bytes_for_wide_part = 0,
    index_granularity = 8192, prewarm_mark_cache = 0, prewarm_primary_key_cache = 0"

$CLICKHOUSE_CLIENT -q "SYSTEM STOP MERGES t"
# 6.4 MB of data: more than the probationary queue, less than the cache.
$CLICKHOUSE_CLIENT -q "INSERT INTO t SELECT number, number FROM numbers(400000)"
$CLICKHOUSE_CLIENT -q "SYSTEM DROP FILESYSTEM CACHE '$cache_name'"

read_settings="
    enable_filesystem_cache = 1,
    read_from_filesystem_cache_if_exists_otherwise_bypass_cache = 0,
    force_read_through_distributed_cache = 0,
    use_uncompressed_cache = 0,
    use_columns_cache = 0,
    use_query_condition_cache = 0,
    use_page_cache_for_disks_without_file_cache = 0,
    use_page_cache_for_local_disks = 0,
    use_page_cache_for_object_storage = 0,
    enable_parallel_replicas = 0,
    max_threads = 1,
    local_filesystem_read_prefetch = 0,
    remote_filesystem_read_prefetch = 0,
    allow_prefetched_read_pool_for_remote_filesystem = 0"

for read in 1 2; do
    $CLICKHOUSE_CLIENT --query_id "${qid_prefix}_${read}" -q "SELECT * FROM t FORMAT Null SETTINGS $read_settings"
done

# The first read evicts nothing, so the second one reads everything from the cache.
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
    SELECT
        replaceOne(query_id, '${qid_prefix}_', '') AS read,
        ProfileEvents['FilesystemCacheEvictedFileSegments'] AS evicted,
        ProfileEvents['CachedReadBufferReadFromSourceBytes'] = 0 AS all_from_cache
    FROM system.query_log
    WHERE current_database = currentDatabase()
        AND type = 'QueryFinish'
        AND query_id IN ('${qid_prefix}_1', '${qid_prefix}_2')
    ORDER BY event_time_microseconds"

$CLICKHOUSE_CLIENT -q "DROP TABLE t"
