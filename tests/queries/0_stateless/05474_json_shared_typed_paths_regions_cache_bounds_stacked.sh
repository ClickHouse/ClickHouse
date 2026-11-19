#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A read of one SHARED typed path of a v2 part on a cache over a cache: both caches keep their file segments within the
# path's region, none aligned below the region's start or past its end.
ALIGNMENT=$((1024 * 1024))

# The region of b is the one contiguous block of cached segments over 1 MiB; the header and marks slices are small
# isolated segments elsewhere in the file. The region is found as the largest contiguous island of segments. Its min
# begin must not be 1 MiB-aligned: without bounds the cache would align the segment down to the 1 MiB boundary below
# the region, and that first segment is always cached, so this is a stable witness that the bounds clamp the read on
# both caches. The region's end is not checked: its last segment is clamped to a non-aligned offset too, but prefetch
# or eviction can leave it uncached, which would make the max end fall on an interior aligned boundary.
outer_cache="${CLICKHOUSE_TEST_UNIQUE_NAME}_outer"
inner_cache="${CLICKHOUSE_TEST_UNIQUE_NAME}_inner"

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS t_cache_bounds_stacked;
CREATE TABLE t_cache_bounds_stacked (x UInt64, json JSON(SHARED a String, SHARED b String, SHARED c String))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
         object_typed_shared_data_serialization_version = 'v2',
         disk = disk(type = cache,
                     max_size = '1Gi',
                     max_file_segment_size = '40Mi',
                     boundary_alignment = '1Mi',
                     cache_on_write_operations = 0,
                     path = '$outer_cache',
                     name = '$outer_cache',
                     disk = disk(type = cache,
                                 max_size = '1Gi',
                                 max_file_segment_size = '40Mi',
                                 boundary_alignment = '1Mi',
                                 cache_on_write_operations = 0,
                                 path = '$inner_cache',
                                 name = '$inner_cache',
                                 disk = 's3_disk'));

-- Regions of about 4 MB each, so that the region of b starts and ends away from the 1 MiB alignment.
INSERT INTO t_cache_bounds_stacked SELECT number, concat('{\"a\":\"', hex(randomString(500)), '\",\"b\":\"', hex(randomString(500)), '\",\"c\":\"', hex(randomString(500)), '\"}') FROM numbers(4000);

SYSTEM DROP FILESYSTEM CACHE '$outer_cache';
SYSTEM DROP FILESYSTEM CACHE '$inner_cache';

SET enable_filesystem_cache = 1, read_from_filesystem_cache_if_exists_otherwise_bypass_cache = 0, force_read_through_distributed_cache = 0,
    enable_reads_from_columns_cache = 0;
SELECT json.b FROM t_cache_bounds_stacked FORMAT Null;
"

# The region is the only data of more than 1 MiB the read touches: the others are the header and a marks slice.
$CLICKHOUSE_CLIENT -m -q "
SELECT cache, count() > 0, min(rb) % $ALIGNMENT != 0
FROM (
    SELECT cache, min(b) AS rb, sum(sz) AS total
    FROM (
        SELECT cache, file_segment_range_begin AS b, file_segment_range_end AS e, size AS sz,
               sum(if(file_segment_range_begin > prev_e + 1, 1, 0)) OVER (PARTITION BY cache ORDER BY file_segment_range_begin) AS island
        FROM (
            SELECT if(cache_name = '$outer_cache', 'outer', 'inner') AS cache, file_segment_range_begin, file_segment_range_end, size,
                   lagInFrame(file_segment_range_end, 1, toUInt64(0)) OVER (PARTITION BY cache ORDER BY file_segment_range_begin) AS prev_e
            FROM system.filesystem_cache WHERE cache_name IN ('$outer_cache', '$inner_cache'))
    ) GROUP BY cache, island HAVING total > $ALIGNMENT
) GROUP BY cache ORDER BY cache DESC;
"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_cache_bounds_stacked"
