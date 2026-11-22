-- A block served from the columns cache, or read to be written to it as a whole stripe, must not be
-- much larger than the read asked for: for wide rows `preferred_block_size_bytes` limits a block to
-- a granule here, and a stripe of the columns cache holds eight of them.
-- The table is small enough for a query to write it to the columns cache of the test configuration.

DROP TABLE IF EXISTS t_columns_cache_wide_rows;
CREATE TABLE t_columns_cache_wide_rows (s String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
INSERT INTO t_columns_cache_wide_rows SELECT repeat('a', 500) FROM numbers(65536);

-- `clickhouse-test` randomizes the reads from and writes to the columns cache: the repeat read hits only with both.
SET use_columns_cache = 1, enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 1;
SET max_threads = 1, max_block_size = 65536, preferred_block_size_bytes = 1000000;

-- The first read fills the cache, the second one is served from it.
SELECT max(blockSize()) <= 16384, sum(cityHash64(s)) FROM t_columns_cache_wide_rows SETTINGS log_comment = '05768_first';
SELECT max(blockSize()) <= 16384, sum(cityHash64(s)) FROM t_columns_cache_wide_rows SETTINGS log_comment = '05768_second';
SELECT max(blockSize()) <= 16384, sum(cityHash64(s)) FROM t_columns_cache_wide_rows SETTINGS use_columns_cache = 0;

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['ColumnsCacheHits'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05768_first', '05768_second')
ORDER BY log_comment;

DROP TABLE t_columns_cache_wide_rows;
