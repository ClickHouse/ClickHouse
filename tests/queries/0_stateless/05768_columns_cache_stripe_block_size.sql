-- A block served from the columns cache, or read to be written to it as a whole stripe, must not be
-- much larger than the read asked for: for wide rows `preferred_block_size_bytes` limits a block to
-- a granule here, and a stripe of the columns cache holds eight of them.

DROP TABLE IF EXISTS t_columns_cache_wide_rows;
CREATE TABLE t_columns_cache_wide_rows (s String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
INSERT INTO t_columns_cache_wide_rows SELECT repeat('a', 1000) FROM numbers(65536);

SET use_columns_cache = 1, max_threads = 1, max_block_size = 65536, preferred_block_size_bytes = 1000000;

-- The first read fills the cache, the second one is served from it.
SELECT max(blockSize()) <= 16384, sum(cityHash64(s)) FROM t_columns_cache_wide_rows;
SELECT max(blockSize()) <= 16384, sum(cityHash64(s)) FROM t_columns_cache_wide_rows;
SELECT max(blockSize()) <= 16384, sum(cityHash64(s)) FROM t_columns_cache_wide_rows SETTINGS use_columns_cache = 0;

DROP TABLE t_columns_cache_wide_rows;
