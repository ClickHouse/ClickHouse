-- Tags: no-parallel
-- no-parallel: the repeat read must find what the first read wrote to the columns cache, which is server-wide.
-- `SYSTEM DROP COLUMNS CACHE` drops the entries of every test, and the tests that fill the cache at the same time
-- lower its admission probability, so that the first read may write nothing to it.

-- A block served from the columns cache, or read to be written to it as a whole stripe, must not be
-- much larger than the read asked for: `preferred_block_size_bytes` limits a block to a granule here,
-- and a stripe of the columns cache holds eight of them.

DROP TABLE IF EXISTS t_columns_cache_wide_rows;
CREATE TABLE t_columns_cache_wide_rows (s String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;
INSERT INTO t_columns_cache_wide_rows SELECT repeat('a', 16) FROM numbers(65536);

-- `clickhouse-test` randomizes the reads from and writes to the columns cache: the repeat read hits only with both.
SET use_columns_cache = 1, enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 1;
SET max_threads = 1, max_block_size = 65536, preferred_block_size_bytes = 32768;
-- `enable_parallel_replicas = 0`: with parallel replicas another replica may do the reads,
-- and their `ProfileEvents` never reach the `query_log` rows checked below.
SET enable_parallel_replicas = 0;

-- The tests that ran before may have lowered the admission probability of the cache; dropping the cache resets it.
SYSTEM DROP COLUMNS CACHE;

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

-- With `preferred_max_column_in_block_size_bytes` too, the rows of a stripe that pass `PREWHERE` stay within the
-- filtered block: the pass ratio of `PREWHERE` is applied once. One row in 8 passes, with a wide value.
DROP TABLE IF EXISTS t_columns_cache_max_column;
CREATE TABLE t_columns_cache_max_column (d UInt8, s String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 1, use_const_adaptive_granularity = 0, enable_block_number_column = 0,
    enable_block_offset_column = 0, string_serialization_version = 'single_stream';
INSERT INTO t_columns_cache_max_column SELECT number % 8, if(number % 8 = 0, repeat('x', 150), '') FROM numbers(1000000);
OPTIMIZE TABLE t_columns_cache_max_column FINAL;

SET preferred_block_size_bytes = 10000000, preferred_max_column_in_block_size_bytes = 100000, use_query_condition_cache = 0,
    allow_prefetched_read_pool_for_local_filesystem = 0;
SYSTEM DROP COLUMNS CACHE;

SELECT max(bs) <= 5120, sum(h) FROM (SELECT blockSize() AS bs, cityHash64(s) AS h FROM t_columns_cache_max_column PREWHERE d = 0)
SETTINGS log_comment = '05768_max_column_first';
SELECT max(bs) <= 5120, sum(h) FROM (SELECT blockSize() AS bs, cityHash64(s) AS h FROM t_columns_cache_max_column PREWHERE d = 0)
SETTINGS log_comment = '05768_max_column_second';
SELECT max(bs) <= 5120, sum(h) FROM (SELECT blockSize() AS bs, cityHash64(s) AS h FROM t_columns_cache_max_column PREWHERE d = 0)
SETTINGS use_columns_cache = 0;

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['ColumnsCacheHits'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05768_max_column_first', '05768_max_column_second')
ORDER BY log_comment;

DROP TABLE t_columns_cache_max_column;
