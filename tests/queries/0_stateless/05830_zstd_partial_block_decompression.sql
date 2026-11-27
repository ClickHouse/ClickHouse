-- A read that needs only the beginning of a ZSTD-compressed block decompresses only the ZSTD blocks of its frame up to
-- the needed data; later reads of the same block continue the frame.

SET max_threads = 1, use_uncompressed_cache = 0, use_query_condition_cache = 0, use_columns_cache = 0,
    enable_parallel_replicas = 0, merge_tree_min_rows_for_seek = 0, merge_tree_min_bytes_for_seek = 0;

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t2;

CREATE TABLE t (k UInt32, f UInt8, s String CODEC(ZSTD(1)))
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0,
    min_compress_block_size = 65536, max_compress_block_size = 1048576,
    serialization_info_version = 'with_types', string_serialization_version = 'with_size_stream',
    ratio_of_defaults_for_sparse_serialization = 1;

-- 16 granules with about 850 KB of `s` each: one compressed block per granule, at least 7 ZSTD blocks. Every row of every
-- fourth granule passes `f = 1`, only the first 100 rows of the others.
INSERT INTO t SELECT number, intDiv(number, 8192) % 4 = 0 OR number % 8192 < 100, concat(toString(number), ':', repeat(hex(cityHash64(number)), 6))
FROM numbers(16 * 8192);
OPTIMIZE TABLE t FINAL;

SELECT count(), sum(cityHash64(s)) FROM t PREWHERE f = 1 SETTINGS log_comment = 'prefix';
SELECT count(), sum(cityHash64(s)) FROM t WHERE f = 1 SETTINGS optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0;
SELECT count(), sum(cityHash64(s)) FROM t SETTINGS log_comment = 'full';

-- The prefix read decompresses less than half of what the full read does.
SYSTEM FLUSH LOGS query_log;
SELECT 2 * sumIf(ProfileEvents['CompressedReadBufferBytes'], log_comment = 'prefix')
    < sumIf(ProfileEvents['CompressedReadBufferBytes'], log_comment = 'full')
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('prefix', 'full');

-- Blocks read to the end in steps.
SELECT count(), sum(cityHash64(s)) FROM t SETTINGS max_block_size = 1000;
-- Granules read to the end in steps, then granules read only in their first rows, in the same stream.
SELECT count(), sum(cityHash64(s)) FROM t PREWHERE f = 1 SETTINGS max_block_size = 1000;

-- About 10 granules per compressed block: the reader seeks inside a partially decompressed block.
CREATE TABLE t2 (k UInt32, f UInt8, s String CODEC(ZSTD(1)))
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 1024, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0,
    min_compress_block_size = 1048576, max_compress_block_size = 1048576,
    serialization_info_version = 'with_types', string_serialization_version = 'with_size_stream',
    ratio_of_defaults_for_sparse_serialization = 1;
INSERT INTO t2 SELECT * FROM t;
OPTIMIZE TABLE t2 FINAL;

SELECT k, cityHash64(s) FROM t2 WHERE k IN (5, 3500, 7300, 9000) ORDER BY k;

DROP TABLE t;
DROP TABLE t2;
