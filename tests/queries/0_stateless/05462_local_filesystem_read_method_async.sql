-- Covers the two asynchronous branches of the local read buffer factory
-- createReadBufferFromFileBase (src/Disks/IO/createReadBufferFromFileBase.cpp):
-- 'io_uring' and 'pread_fake_async'.
-- Expected behaviour (docs/en/operations/settings/settings.md, setting
-- local_filesystem_read_method): the setting only selects the method used to
-- read local files, so the query result must not depend on it.

DROP TABLE IF EXISTS t_lfs_read_method;

-- min_bytes_for_wide_part / min_bytes_for_full_part_storage = 0 give a wide part in
-- full storage, so every column is a separate local file and each column stream asks
-- the factory for a read buffer of the chosen method. Small granules give several marks.
CREATE TABLE t_lfs_read_method (a UInt64, s String)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_lfs_read_method SELECT number, toString(number) FROM numbers(64);

-- min_bytes_to_use_direct_io = 0 and min_bytes_to_use_mmap_io = 0 keep the O_DIRECT and
-- the mmap branch of the factory out of the way; the caches are off so that the second
-- read really goes to the files again.
SELECT count(), sum(a), max(s) FROM t_lfs_read_method
SETTINGS local_filesystem_read_method = 'io_uring',
    min_bytes_to_use_direct_io = 0,
    min_bytes_to_use_mmap_io = 0,
    local_filesystem_read_prefetch = 0,
    use_uncompressed_cache = 0,
    use_columns_cache = 0;

SELECT count(), sum(a), max(s) FROM t_lfs_read_method
SETTINGS local_filesystem_read_method = 'pread_fake_async',
    min_bytes_to_use_direct_io = 0,
    min_bytes_to_use_mmap_io = 0,
    local_filesystem_read_prefetch = 0,
    use_uncompressed_cache = 0,
    use_columns_cache = 0;

DROP TABLE t_lfs_read_method;
