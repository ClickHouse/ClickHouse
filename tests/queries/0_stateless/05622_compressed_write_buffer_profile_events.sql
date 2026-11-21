-- The ProfileEvents of `CompressedWriteBuffer` count the blocks and the uncompressed bytes it compresses.

DROP TABLE IF EXISTS t_compressed_write_buffer;
CREATE TABLE t_compressed_write_buffer (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_compressed_write_buffer SETTINGS async_insert = 0 SELECT number FROM numbers(100000);

SYSTEM FLUSH LOGS query_log;
SELECT
    ProfileEvents['CompressedWriteBufferBlocks'] > 0,
    ProfileEvents['CompressedWriteBufferBytes'] >= 800000
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Insert'
    AND has(tables, currentDatabase() || '.t_compressed_write_buffer');

DROP TABLE t_compressed_write_buffer;
