-- A read through the columns cache keeps the block size that `preferred_block_size_bytes` asks for when
-- the rows are wide. It used to read a whole stripe of the cache (65536 rows) as one block, which is tens
-- of megabytes for such rows: the functions over the block then worked out of the CPU cache and the read
-- was slower with the cache than without it.

DROP TABLE IF EXISTS t_columns_cache_wide_rows;

CREATE TABLE t_columns_cache_wide_rows (id UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0;

INSERT INTO t_columns_cache_wide_rows
SELECT number, repeat(toString(number % 10), 400) FROM numbers(70000)
SETTINGS max_insert_threads = 1, max_block_size = 70000, min_insert_block_size_rows = 70000, min_insert_block_size_bytes = 0;

SET use_columns_cache = 1, enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 1;
SET max_threads = 1, max_block_size = 65409, preferred_block_size_bytes = 1000000, use_query_condition_cache = 0;

-- A row is about 400 bytes, so a block of 1 MB is less than a granule, and the read asks for a granule.
-- The first read writes the cache and the next ones are served from it; none reads a block of a stripe.
SELECT max(blockSize()) <= 16384, sum(cityHash64(s)) FROM t_columns_cache_wide_rows;
SELECT max(blockSize()) <= 16384, sum(cityHash64(s)) FROM t_columns_cache_wide_rows;
SELECT max(blockSize()) <= 16384, sum(cityHash64(s)) FROM t_columns_cache_wide_rows;

DROP TABLE t_columns_cache_wide_rows;
