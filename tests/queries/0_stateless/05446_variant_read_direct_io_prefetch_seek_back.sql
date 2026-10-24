DROP TABLE IF EXISTS t_variant_direct_io_prefetch;

CREATE TABLE t_variant_direct_io_prefetch (id UInt64, v Variant(String, UInt64) CODEC(LZ4))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    index_granularity = 8192, index_granularity_bytes = '10Mi', min_compress_block_size = 65536, max_compress_block_size = 1048576,
    serialization_info_version = 'with_types', string_serialization_version = 'with_size_stream',
    propagate_types_serialization_versions_to_nested_types = 1;

-- Strings only in granules 0 and 5: the `v.String` sizes file is under 4 KiB, so with direct IO every mark of it is read
-- from file offset 0. Reading in reverse order takes the granule without strings first, then the one at mark 0.
INSERT INTO t_variant_direct_io_prefetch SELECT number, multiIf(
    number < 8192, CAST('str_' || toString(number), 'Variant(String, UInt64)'),
    number >= 40960, CAST('long_string_' || toString(number), 'Variant(String, UInt64)'),
    CAST(number, 'Variant(String, UInt64)')) FROM numbers(49152);
OPTIMIZE TABLE t_variant_direct_io_prefetch FINAL;

SELECT count(), any(part_type) FROM system.parts
WHERE database = currentDatabase() AND table = 't_variant_direct_io_prefetch' AND active;

SELECT count(s), sum(length(s)), sum(startsWith(s, 'str_')) FROM
(
    SELECT v.String AS s FROM t_variant_direct_io_prefetch WHERE id < 8192 OR (id >= 16384 AND id < 24576) ORDER BY id DESC
)
SETTINGS max_threads = 1, optimize_read_in_order = 1, query_plan_remove_redundant_sorting = 0, optimize_move_to_prewhere = 0,
    min_bytes_to_use_direct_io = 1, local_filesystem_read_method = 'pread_threadpool', local_filesystem_read_prefetch = 1,
    use_uncompressed_cache = 0;

-- The same with only 1000 strings in granule 5.
TRUNCATE TABLE t_variant_direct_io_prefetch;
INSERT INTO t_variant_direct_io_prefetch SELECT number, if(number < 8192 OR number >= 40960,
    CAST('str_' || toString(number), 'Variant(String, UInt64)'), CAST(number, 'Variant(String, UInt64)')) FROM numbers(41960);
OPTIMIZE TABLE t_variant_direct_io_prefetch FINAL;

SELECT count(), any(part_type) FROM system.parts
WHERE database = currentDatabase() AND table = 't_variant_direct_io_prefetch' AND active;

SELECT count(s), sum(length(s)), sum(startsWith(s, 'str_')) FROM
(
    SELECT v.String AS s FROM t_variant_direct_io_prefetch WHERE id < 8192 OR (id >= 16384 AND id < 24576) ORDER BY id DESC
)
SETTINGS max_threads = 1, optimize_read_in_order = 1, query_plan_remove_redundant_sorting = 0, optimize_move_to_prewhere = 0,
    min_bytes_to_use_direct_io = 1, local_filesystem_read_method = 'pread_threadpool', local_filesystem_read_prefetch = 1,
    use_uncompressed_cache = 0;

DROP TABLE t_variant_direct_io_prefetch;
