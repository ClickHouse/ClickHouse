#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A read of one SHARED typed path of a v2 part reads the path's region of the shared region file through a view. The
# filesystem cache keeps its file segments within the region: none is aligned below the region's start or past its end,
# which would download the neighbor paths' regions. Both for a part with a file per stream and for a packed part, whose
# region view lies inside the view of the region file in the packed file.
ALIGNMENT=$((1024 * 1024))

# The region of b is the one contiguous block of cached segments over 1 MiB; the header and marks slices are small
# isolated segments elsewhere in the file. The region's own edge segments can be smaller than 1 MiB when the read is
# fragmented, so the region is found as the largest contiguous island of segments rather than filtered by size; its
# min begin and max end are then the region's true bounds, which must not be 1 MiB-aligned.

for storage in full packed; do
    cache_name="${CLICKHOUSE_TEST_UNIQUE_NAME}_${storage}"
    if [ "$storage" = full ]; then full_part_storage_bytes=0; else full_part_storage_bytes=1073741824; fi

    $CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS t_cache_bounds_$storage;
    CREATE TABLE t_cache_bounds_$storage (x UInt64, json JSON(SHARED a String, SHARED b String, SHARED c String))
    ENGINE = MergeTree ORDER BY x
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = $full_part_storage_bytes,
             object_typed_shared_data_serialization_version = 'v2',
             disk = disk(type = cache,
                         max_size = '1Gi',
                         max_file_segment_size = '40Mi',
                         boundary_alignment = '1Mi',
                         cache_on_write_operations = 0,
                         path = '$cache_name',
                         name = '$cache_name',
                         disk = 's3_disk');

    -- Regions of about 4 MB each, so that the region of b starts and ends away from the 1 MiB alignment.
    INSERT INTO t_cache_bounds_$storage SELECT number, concat('{\"a\":\"', hex(randomString(500)), '\",\"b\":\"', hex(randomString(500)), '\",\"c\":\"', hex(randomString(500)), '\"}') FROM numbers(4000);

    SYSTEM DROP FILESYSTEM CACHE '$cache_name';

    SET enable_filesystem_cache = 1, read_from_filesystem_cache_if_exists_otherwise_bypass_cache = 0, force_read_through_distributed_cache = 0,
        enable_reads_from_columns_cache = 0;
    SELECT json.b FROM t_cache_bounds_$storage FORMAT Null;
    "

    # The region is the only data of more than 1 MiB the read touches: the others are the header and a marks slice.
    $CLICKHOUSE_CLIENT -m -q "
    SELECT
        '$storage',
        any(part_storage_type) FROM system.parts WHERE database = currentDatabase() AND table = 't_cache_bounds_$storage' AND active;
    SELECT count() > 0, min(rb) % $ALIGNMENT != 0, (max(re) + 1) % $ALIGNMENT != 0
    FROM (
        SELECT min(b) AS rb, max(e) AS re, sum(sz) AS total
        FROM (
            SELECT file_segment_range_begin AS b, file_segment_range_end AS e, size AS sz,
                   sum(if(file_segment_range_begin > prev_e + 1, 1, 0)) OVER (ORDER BY file_segment_range_begin) AS island
            FROM (
                SELECT file_segment_range_begin, file_segment_range_end, size,
                       lagInFrame(file_segment_range_end, 1, toUInt64(0)) OVER (ORDER BY file_segment_range_begin) AS prev_e
                FROM system.filesystem_cache WHERE cache_name = '$cache_name')
        ) GROUP BY island HAVING total > $ALIGNMENT);
    "

    $CLICKHOUSE_CLIENT -q "DROP TABLE t_cache_bounds_$storage"
done
