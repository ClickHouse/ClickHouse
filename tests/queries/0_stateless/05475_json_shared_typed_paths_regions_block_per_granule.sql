-- A granule of a path's region starts a new compressed block once the open block holds min_compress_block_size, as in a
-- column's own stream, so that the read tasks of a region do not decompress each other's granules: a read of one path
-- split into many tasks decompresses the path's data once.

DROP TABLE IF EXISTS t_region_blocks;

CREATE TABLE t_region_blocks (x UInt64, json JSON(SHARED a UInt64, SHARED b String))
ENGINE = MergeTree ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, object_typed_shared_data_serialization_version = 'v2',
         index_granularity = 8192, index_granularity_bytes = 10485760, min_compress_block_size = 65536,
         max_compress_block_size = 1048576;
INSERT INTO t_region_blocks SELECT number, concat('{"a":', toString(number), ',"b":"b', toString(number % 1000), '"}') FROM numbers(100000);
OPTIMIZE TABLE t_region_blocks FINAL;

SELECT sum(json.a) FROM t_region_blocks
SETTINGS max_threads = 8, merge_tree_min_rows_for_concurrent_read = 8192, merge_tree_min_bytes_for_concurrent_read = 1,
         enable_reads_from_columns_cache = 0, use_uncompressed_cache = 0, enable_parallel_replicas = 0,
         log_comment = '05475_region_read';

SYSTEM FLUSH LOGS query_log;

-- The marks and the header add a few KiB to the path's data.
SELECT ProfileEvents['CompressedReadBufferBytes'] < 1.1 * (
    SELECT sum(subcolumns.data_uncompressed_bytes) FROM system.parts_columns ARRAY JOIN subcolumns
    WHERE database = currentDatabase() AND table = 't_region_blocks' AND active AND column = 'json' AND subcolumns.names = 'a')
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05475_region_read';

DROP TABLE t_region_blocks;
