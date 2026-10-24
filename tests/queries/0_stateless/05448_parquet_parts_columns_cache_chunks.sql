-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database, no-fasttest
-- Tag no-parallel: drops the columns cache.
-- Tag no-fasttest: Depends on Parquet

-- The columns cache keeps a column of a row group in chunks of 65536 rows. A row group of a Parquet part larger
-- than a chunk is decoded a chunk at a time while it uses the cache, even when the blocks are smaller or not aligned.

SET max_threads = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_parquet_cache_chunks;

CREATE TABLE t_parquet_cache_chunks (id UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_parquet_part = 1, parquet_row_group_granularity_rows = 200000, parquet_row_group_granularity_bytes = 1000000000;

INSERT INTO t_parquet_cache_chunks SELECT number, toString(number) FROM numbers(500000)
SETTINGS max_block_size = 1000000, min_insert_block_size_rows = 1000000;

SYSTEM DROP COLUMNS CACHE;

SELECT part_type, marks FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_cache_chunks' AND active;

SELECT 'without cache', sum(id), sum(length(s)) FROM t_parquet_cache_chunks SETTINGS use_columns_cache = 0;
SELECT 'populate', sum(id), sum(length(s)) FROM t_parquet_cache_chunks SETTINGS use_columns_cache = 1, max_block_size = 10000;

SELECT 'entries';
SELECT column, count(), sum(rows), min(rows), max(rows) FROM system.columns_cache
WHERE database = currentDatabase() AND table = 't_parquet_cache_chunks' GROUP BY column ORDER BY column;

SELECT 'from cache', sum(id), sum(length(s)) FROM t_parquet_cache_chunks SETTINGS use_columns_cache = 1, max_block_size = 7777;
SELECT 'from cache with filter', sum(id), count() FROM t_parquet_cache_chunks WHERE id % 7 = 3 SETTINGS use_columns_cache = 1;
SELECT 'from cache, range', sum(id), count() FROM t_parquet_cache_chunks WHERE id BETWEEN 130000 AND 270000 SETTINGS use_columns_cache = 1;
SELECT 'without cache, range', sum(id), count() FROM t_parquet_cache_chunks WHERE id BETWEEN 130000 AND 270000 SETTINGS use_columns_cache = 0;

DROP TABLE t_parquet_cache_chunks;
