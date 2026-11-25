#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# As 05455, on an encrypted disk over a cache: the decryption passes the bounds of the path's region down to the cache,
# shifted by the encryption header, so the cache keeps its file segments within the region.
ALIGNMENT=$((1024 * 1024))

for storage in full packed; do
    cache_name="${CLICKHOUSE_TEST_UNIQUE_NAME}_${storage}"
    if [ "$storage" = full ]; then full_part_storage_bytes=0; else full_part_storage_bytes=1073741824; fi

    $CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS t_cache_bounds_encrypted_$storage;
    CREATE TABLE t_cache_bounds_encrypted_$storage (x UInt64, json JSON(SHARED a String, SHARED b String, SHARED c String))
    ENGINE = MergeTree ORDER BY x
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = $full_part_storage_bytes,
             object_typed_shared_data_serialization_version = 'v2',
             disk = disk(type = encrypted,
                         key = '1234567812345678',
                         path = '${cache_name}_encrypted/',
                         disk = disk(type = cache,
                                     max_size = '1Gi',
                                     max_file_segment_size = '40Mi',
                                     boundary_alignment = '1Mi',
                                     cache_on_write_operations = 0,
                                     path = '$cache_name',
                                     name = '$cache_name',
                                     disk = 's3_disk'));

    -- Regions of about 4 MB each, so that the region of b starts and ends away from the 1 MiB alignment.
    INSERT INTO t_cache_bounds_encrypted_$storage SELECT number, concat('{\"a\":\"', hex(randomString(500)), '\",\"b\":\"', hex(randomString(500)), '\",\"c\":\"', hex(randomString(500)), '\"}') FROM numbers(4000);

    SYSTEM DROP FILESYSTEM CACHE '$cache_name';

    SET enable_filesystem_cache = 1, read_from_filesystem_cache_if_exists_otherwise_bypass_cache = 0, force_read_through_distributed_cache = 0,
        enable_reads_from_columns_cache = 0;
    SELECT json.b FROM t_cache_bounds_encrypted_$storage FORMAT Null;
    "

    # The region of b is the only contiguous island of a file's cached segments with over 1 MiB downloaded, see 05455.
    # Each read of the region file also reads the encryption header at its start, before the bounds are set: a segment
    # at offset 0 with at most one buffer downloaded.
    $CLICKHOUSE_CLIENT -m -q "
    SELECT
        '$storage',
        any(part_storage_type) FROM system.parts WHERE database = currentDatabase() AND table = 't_cache_bounds_encrypted_$storage' AND active;
    SELECT count() = 1, min(rb) % $ALIGNMENT != 0, (max(re) + 1) % $ALIGNMENT != 0
    FROM (
        SELECT min(b) AS rb, max(e) AS re, sum(downloaded) AS total
        FROM (
            SELECT key, file_segment_range_begin AS b, file_segment_range_end AS e, downloaded_size AS downloaded,
                   sum(if(file_segment_range_begin > prev_e + 1, 1, 0)) OVER (PARTITION BY key ORDER BY file_segment_range_begin) AS island
            FROM (
                SELECT key, file_segment_range_begin, file_segment_range_end, downloaded_size,
                       lagInFrame(file_segment_range_end, 1, toUInt64(0)) OVER (PARTITION BY key ORDER BY file_segment_range_begin) AS prev_e
                FROM system.filesystem_cache WHERE cache_name = '$cache_name')
        ) GROUP BY key, island HAVING total > $ALIGNMENT);
    "

    $CLICKHOUSE_CLIENT -q "DROP TABLE t_cache_bounds_encrypted_$storage"
done
