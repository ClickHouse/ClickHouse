#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# - no-fasttest: uses a filesystem cache disk
# - no-replicated-database: the replicas would share the disk's metadata_path

# A file inside a packed part's `data.packed` archive must be read with buffers sized for that file, as
# the same file of a full-storage part is, not for the whole archive.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

columns=$(for i in $(seq 1 100); do echo -n ", c$i String"; done)
values=$(for i in $(seq 1 100); do echo -n ", toString(cityHash64(number, $i))"; done)
nonce=$(random_str 10)
declare -A hash

function make_disk()
{
    local name="${CLICKHOUSE_TEST_UNIQUE_NAME}_$1"
    local blob="disk(type = 'local_blob_storage', path = '${CLICKHOUSE_DISKS_FILES}/${name}/',
        metadata_path = '${CLICKHOUSE_DISKS_FILES}/${name}_metadata/')"
    blob="disk(type = cache, name = '${name}', path = '${name}/', max_size = '1Gi', disk = ${blob})"
    echo "disk(type = 'encrypted', key = '1234567812345678', disk = ${blob})"
}

read_settings="max_threads = 1, remote_filesystem_read_prefetch = 1, enable_filesystem_cache = 1,
    filesystem_cache_prefer_bigger_buffer_size = 1, read_from_filesystem_cache_if_exists_otherwise_bypass_cache = 0,
    prefetch_buffer_size = 1048576, max_read_buffer_size = 1048576,
    allow_prefetched_read_pool_for_remote_filesystem = 0, use_uncompressed_cache = 0, use_columns_cache = 0,
    use_page_cache_for_disks_without_file_cache = 0, use_page_cache_for_object_storage = 0, use_page_cache_for_local_disks = 0,
    force_read_through_distributed_cache = 0, enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0,
    local_filesystem_read_method = 'pread', local_filesystem_read_prefetch = 0, min_bytes_to_use_mmap_io = 0,
    min_bytes_to_use_direct_io = 0, use_query_condition_cache = 0"

for storage in packed full; do
    if [ "$storage" = "packed" ]; then full_storage_bytes="'10Gi'"; else full_storage_bytes=0; fi
    $CLICKHOUSE_CLIENT -m -q "
        CREATE TABLE t_${storage} (id UInt64 ${columns}) ENGINE = MergeTree ORDER BY id
        SETTINGS disk = $(make_disk "$storage"), min_bytes_for_wide_part = 0,
            min_bytes_for_full_part_storage = ${full_storage_bytes};
        INSERT INTO t_${storage} SELECT number ${values} FROM numbers(2000)
        SETTINGS async_insert = 0, max_insert_threads = 1, max_block_size = 65536, max_insert_block_size = 1048576,
            min_insert_block_size_rows = 1048576, min_insert_block_size_bytes = 268435456;
        SELECT '${storage}', count(), any(part_type), any(part_storage_type)
        FROM system.parts WHERE database = currentDatabase() AND table = 't_${storage}' AND active;"
done

for method in threadpool read; do
    for storage in packed full; do
        hash[$storage]=$($CLICKHOUSE_CLIENT --query_id "${nonce}_${storage}_${method}" -q "
            SELECT sum(cityHash64(*)) FROM t_${storage}
            SETTINGS remote_filesystem_read_method = '${method}', ${read_settings}")
    done
    echo "${method} hashes equal: $([ -n "${hash[packed]}" ] && [ "${hash[packed]}" = "${hash[full]}" ] && echo 1 || echo 0)"
done

# Bytes allocated for read buffers: the packed part stays within 4x of the full one.
$CLICKHOUSE_CLIENT -m -q "
    SYSTEM FLUSH LOGS query_log;
    SELECT p[3] AS method, sumIf(alloc, p[2] = 'packed') <= 4 * sumIf(alloc, p[2] = 'full')
    FROM
    (
        SELECT splitByChar('_', query_id) AS p, ProfileEvents['IOBufferAllocBytes'] AS alloc
        FROM system.query_log
        WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
            AND startsWith(query_id, '${nonce}_')
    )
    GROUP BY method ORDER BY method;"

for storage in packed full; do
    $CLICKHOUSE_CLIENT -q "DROP TABLE t_${storage}"
done
