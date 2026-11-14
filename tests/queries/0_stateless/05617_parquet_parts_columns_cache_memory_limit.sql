-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database, no-fasttest
-- Tag no-parallel: drops the columns cache.
-- Tag no-fasttest: Depends on Parquet

-- A row group of a `Parquet` part that uses the columns cache is decoded in whole chunks of 65536 rows, unless a whole
-- chunk would take a large part of the memory the query may use: then only the requested rows are decoded, and the
-- row group is not written to the cache.

SET allow_experimental_parquet_parts = 1;
SET max_threads = 1;
SET enable_parallel_replicas = 0;
SET use_indexes_refiner_in_read_pools = 0;

DROP TABLE IF EXISTS t_parquet_cc_memory;

CREATE TABLE t_parquet_cc_memory (id UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_parquet_part = 1, parquet_row_group_granularity_rows = 200000, parquet_row_group_granularity_bytes = 1000000000;

INSERT INTO t_parquet_cc_memory SELECT number, repeat('x', 1024) FROM numbers(100000)
SETTINGS max_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;

SELECT part_type, marks FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_cc_memory' AND active;

SYSTEM DROP COLUMNS CACHE;
SELECT length(s) FROM t_parquet_cc_memory LIMIT 1
SETTINGS use_columns_cache = 1, max_block_size = 1, max_memory_usage = 33554432;
SELECT 'limited', count() FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_parquet_cc_memory';

SELECT count(), sum(length(s)) FROM t_parquet_cc_memory
SETTINGS use_columns_cache = 1, max_block_size = 1000, max_memory_usage = 33554432;

SELECT 'limited full', count() FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_parquet_cc_memory';

-- Without the memory limit, the whole chunks are decoded and written to the cache.
SELECT count(), sum(length(s)) FROM t_parquet_cc_memory SETTINGS use_columns_cache = 1;
SELECT 'unlimited', column, count(), sum(rows) FROM system.columns_cache
WHERE database = currentDatabase() AND table = 't_parquet_cc_memory' GROUP BY column ORDER BY column;

-- The rows requested under the memory limit are taken from the cached chunks.
SELECT length(s) FROM t_parquet_cc_memory LIMIT 1
SETTINGS use_columns_cache = 1, max_block_size = 1, max_memory_usage = 33554432;
SELECT sum(length(s)) FROM t_parquet_cc_memory WHERE id >= 99990
SETTINGS use_columns_cache = 1, max_block_size = 1, max_memory_usage = 33554432;

DROP TABLE t_parquet_cc_memory;
