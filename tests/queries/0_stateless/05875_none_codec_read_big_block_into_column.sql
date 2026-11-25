-- A block of a CODEC(NONE) or Quantized column that is larger than the read buffer is read straight into the column
-- (CompressedReadBufferFromFile::readBig for MergeTree, CompressedReadBuffer::readBig for Log).
-- Every line compares what a read method returns with the same values generated from numbers().

SET enable_quantized_codec = 1;

DROP TABLE IF EXISTS t_none;
DROP TABLE IF EXISTS t_log;
DROP TABLE IF EXISTS t_expected;

-- One block per granule for v, f and q (64 KiB <= granule < 1 MiB), eight granules per block for u.
CREATE TABLE t_none
(
    id UInt64,
    v Array(UInt32) CODEC(NONE),
    f FixedString(256) CODEC(NONE),
    u UInt64 CODEC(NONE),
    q Array(Float32) CODEC(Quantized('rabitq', 64))
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1024, index_granularity_bytes = 10485760, min_compress_block_size = 65536, max_compress_block_size = 1048576,
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_none
SELECT
    number,
    CAST(range(number % 300) AS Array(UInt32)),
    toFixedString(repeat(toString(number % 10), 256), 256),
    number * 3 + 1,
    arrayMap(i -> toFloat32((number * 7 + i) % 997), range(64))
FROM numbers(30000);

OPTIMIZE TABLE t_none FINAL;

-- StorageLog takes max_compress_block_size from the context that creates it.
SET max_compress_block_size = 1048576, min_compress_block_size = 65536;
CREATE TABLE t_log (v Array(UInt32) CODEC(NONE), f FixedString(256) CODEC(NONE)) ENGINE = Log;
INSERT INTO t_log
SELECT CAST(range(number % 300) AS Array(UInt32)), toFixedString(repeat(toString(number % 10), 256), 256)
FROM numbers(30000);

CREATE TABLE t_expected ENGINE = Memory AS
SELECT
    shape,
    (sum(cityHash64(CAST(range(number % 300) AS Array(UInt32)))),
     sum(cityHash64(toFixedString(repeat(toString(number % 10), 256), 256))),
     sum(number * 3 + 1),
     sum(cityHash64(arrayMap(i -> toFloat32((number * 7 + i) % 997), range(64))))) AS h
FROM numbers(30000)
ARRAY JOIN
    [1, number BETWEEN 3001 AND 27000, number = 12345] AS in_shape,
    ['full', 'range', 'point'] AS shape
WHERE in_shape
GROUP BY shape;


-- mmap maps the whole file, so it reads every block in place. The point reads disable PREWHERE, which would read only the
-- matching row.
SELECT 'full pread_threadpool', (SELECT (sum(cityHash64(v)), sum(cityHash64(f)), sum(u), sum(cityHash64(q))) FROM t_none) = (SELECT h FROM t_expected WHERE shape = 'full')
SETTINGS local_filesystem_read_method = 'pread_threadpool', max_read_buffer_size_local_fs = 4097,
    use_uncompressed_cache = 0, use_columns_cache = 0, use_page_cache_for_local_disks = 0, min_bytes_to_use_direct_io = 0, min_bytes_to_use_mmap_io = 0;
SELECT 'range pread_threadpool', (SELECT (sum(cityHash64(v)), sum(cityHash64(f)), sum(u), sum(cityHash64(q))) FROM t_none WHERE id BETWEEN 3001 AND 27000) = (SELECT h FROM t_expected WHERE shape = 'range')
SETTINGS local_filesystem_read_method = 'pread_threadpool', max_read_buffer_size_local_fs = 131072,
    use_uncompressed_cache = 0, use_columns_cache = 0, use_page_cache_for_local_disks = 0, min_bytes_to_use_direct_io = 0, min_bytes_to_use_mmap_io = 0;
SELECT 'point pread_threadpool', (SELECT (sum(cityHash64(v)), sum(cityHash64(f)), sum(u), sum(cityHash64(q))) FROM t_none WHERE id = 12345) = (SELECT h FROM t_expected WHERE shape = 'point')
SETTINGS local_filesystem_read_method = 'pread_threadpool', max_read_buffer_size_local_fs = 4097, optimize_move_to_prewhere = 0,
    use_uncompressed_cache = 0, use_columns_cache = 0, use_page_cache_for_local_disks = 0, min_bytes_to_use_direct_io = 0, min_bytes_to_use_mmap_io = 0;

SELECT 'full pread_fake_async', (SELECT (sum(cityHash64(v)), sum(cityHash64(f)), sum(u), sum(cityHash64(q))) FROM t_none) = (SELECT h FROM t_expected WHERE shape = 'full')
SETTINGS local_filesystem_read_method = 'pread_fake_async', max_read_buffer_size_local_fs = 4097,
    use_uncompressed_cache = 0, use_columns_cache = 0, use_page_cache_for_local_disks = 0, min_bytes_to_use_direct_io = 0, min_bytes_to_use_mmap_io = 0;
SELECT 'range pread_fake_async', (SELECT (sum(cityHash64(v)), sum(cityHash64(f)), sum(u), sum(cityHash64(q))) FROM t_none WHERE id BETWEEN 3001 AND 27000) = (SELECT h FROM t_expected WHERE shape = 'range')
SETTINGS local_filesystem_read_method = 'pread_fake_async', max_read_buffer_size_local_fs = 131072,
    use_uncompressed_cache = 0, use_columns_cache = 0, use_page_cache_for_local_disks = 0, min_bytes_to_use_direct_io = 0, min_bytes_to_use_mmap_io = 0;
SELECT 'point pread_fake_async', (SELECT (sum(cityHash64(v)), sum(cityHash64(f)), sum(u), sum(cityHash64(q))) FROM t_none WHERE id = 12345) = (SELECT h FROM t_expected WHERE shape = 'point')
SETTINGS local_filesystem_read_method = 'pread_fake_async', max_read_buffer_size_local_fs = 4097, optimize_move_to_prewhere = 0,
    use_uncompressed_cache = 0, use_columns_cache = 0, use_page_cache_for_local_disks = 0, min_bytes_to_use_direct_io = 0, min_bytes_to_use_mmap_io = 0;

SELECT 'full pread', (SELECT (sum(cityHash64(v)), sum(cityHash64(f)), sum(u), sum(cityHash64(q))) FROM t_none) = (SELECT h FROM t_expected WHERE shape = 'full')
SETTINGS local_filesystem_read_method = 'pread', max_read_buffer_size_local_fs = 4097,
    use_uncompressed_cache = 0, use_columns_cache = 0, use_page_cache_for_local_disks = 0, min_bytes_to_use_direct_io = 0, min_bytes_to_use_mmap_io = 0;
SELECT 'range pread', (SELECT (sum(cityHash64(v)), sum(cityHash64(f)), sum(u), sum(cityHash64(q))) FROM t_none WHERE id BETWEEN 3001 AND 27000) = (SELECT h FROM t_expected WHERE shape = 'range')
SETTINGS local_filesystem_read_method = 'pread', max_read_buffer_size_local_fs = 131072,
    use_uncompressed_cache = 0, use_columns_cache = 0, use_page_cache_for_local_disks = 0, min_bytes_to_use_direct_io = 0, min_bytes_to_use_mmap_io = 0;
SELECT 'point pread', (SELECT (sum(cityHash64(v)), sum(cityHash64(f)), sum(u), sum(cityHash64(q))) FROM t_none WHERE id = 12345) = (SELECT h FROM t_expected WHERE shape = 'point')
SETTINGS local_filesystem_read_method = 'pread', max_read_buffer_size_local_fs = 4097, optimize_move_to_prewhere = 0,
    use_uncompressed_cache = 0, use_columns_cache = 0, use_page_cache_for_local_disks = 0, min_bytes_to_use_direct_io = 0, min_bytes_to_use_mmap_io = 0;

SELECT 'full read', (SELECT (sum(cityHash64(v)), sum(cityHash64(f)), sum(u), sum(cityHash64(q))) FROM t_none) = (SELECT h FROM t_expected WHERE shape = 'full')
SETTINGS local_filesystem_read_method = 'read', max_read_buffer_size_local_fs = 4097,
    use_uncompressed_cache = 0, use_columns_cache = 0, use_page_cache_for_local_disks = 0, min_bytes_to_use_direct_io = 0, min_bytes_to_use_mmap_io = 0;
SELECT 'range read', (SELECT (sum(cityHash64(v)), sum(cityHash64(f)), sum(u), sum(cityHash64(q))) FROM t_none WHERE id BETWEEN 3001 AND 27000) = (SELECT h FROM t_expected WHERE shape = 'range')
SETTINGS local_filesystem_read_method = 'read', max_read_buffer_size_local_fs = 131072,
    use_uncompressed_cache = 0, use_columns_cache = 0, use_page_cache_for_local_disks = 0, min_bytes_to_use_direct_io = 0, min_bytes_to_use_mmap_io = 0;
SELECT 'point read', (SELECT (sum(cityHash64(v)), sum(cityHash64(f)), sum(u), sum(cityHash64(q))) FROM t_none WHERE id = 12345) = (SELECT h FROM t_expected WHERE shape = 'point')
SETTINGS local_filesystem_read_method = 'read', max_read_buffer_size_local_fs = 4097, optimize_move_to_prewhere = 0,
    use_uncompressed_cache = 0, use_columns_cache = 0, use_page_cache_for_local_disks = 0, min_bytes_to_use_direct_io = 0, min_bytes_to_use_mmap_io = 0;

SELECT 'full mmap', (SELECT (sum(cityHash64(v)), sum(cityHash64(f)), sum(u), sum(cityHash64(q))) FROM t_none) = (SELECT h FROM t_expected WHERE shape = 'full')
SETTINGS local_filesystem_read_method = 'mmap', max_read_buffer_size_local_fs = 4097,
    use_uncompressed_cache = 0, use_columns_cache = 0, use_page_cache_for_local_disks = 0, min_bytes_to_use_direct_io = 0, min_bytes_to_use_mmap_io = 1;
SELECT 'range mmap', (SELECT (sum(cityHash64(v)), sum(cityHash64(f)), sum(u), sum(cityHash64(q))) FROM t_none WHERE id BETWEEN 3001 AND 27000) = (SELECT h FROM t_expected WHERE shape = 'range')
SETTINGS local_filesystem_read_method = 'mmap', max_read_buffer_size_local_fs = 131072,
    use_uncompressed_cache = 0, use_columns_cache = 0, use_page_cache_for_local_disks = 0, min_bytes_to_use_direct_io = 0, min_bytes_to_use_mmap_io = 1;
SELECT 'point mmap', (SELECT (sum(cityHash64(v)), sum(cityHash64(f)), sum(u), sum(cityHash64(q))) FROM t_none WHERE id = 12345) = (SELECT h FROM t_expected WHERE shape = 'point')
SETTINGS local_filesystem_read_method = 'mmap', max_read_buffer_size_local_fs = 4097, optimize_move_to_prewhere = 0,
    use_uncompressed_cache = 0, use_columns_cache = 0, use_page_cache_for_local_disks = 0, min_bytes_to_use_direct_io = 0, min_bytes_to_use_mmap_io = 1;

SELECT 'full O_DIRECT', (SELECT (sum(cityHash64(v)), sum(cityHash64(f)), sum(u), sum(cityHash64(q))) FROM t_none) = (SELECT h FROM t_expected WHERE shape = 'full')
SETTINGS local_filesystem_read_method = 'pread_threadpool', max_read_buffer_size_local_fs = 131072,
    use_uncompressed_cache = 0, use_columns_cache = 0, use_page_cache_for_local_disks = 0, min_bytes_to_use_direct_io = 1, min_bytes_to_use_mmap_io = 0;

SELECT 'log 4097', (SELECT (sum(cityHash64(v)), sum(cityHash64(f))) FROM t_log) = (SELECT (h.1, h.2) FROM t_expected WHERE shape = 'full')
SETTINGS max_read_buffer_size_local_fs = 4097, max_read_buffer_size = 4097;
SELECT 'log 1048576', (SELECT (sum(cityHash64(v)), sum(cityHash64(f))) FROM t_log) = (SELECT (h.1, h.2) FROM t_expected WHERE shape = 'full')
SETTINGS max_read_buffer_size_local_fs = 1048576, max_read_buffer_size = 1048576;

DROP TABLE t_none;
DROP TABLE t_log;
DROP TABLE t_expected;
