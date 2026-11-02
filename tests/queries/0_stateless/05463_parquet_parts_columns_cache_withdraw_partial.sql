-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database, no-fasttest
-- Tag no-parallel: drops the columns cache.
-- Tag no-fasttest: Depends on Parquet

-- The columns cache keeps a column of a row group of a `Parquet` part in chunks of 65536 rows and serves it only
-- with all its chunks. A read that stops before the end of a row group (`LIMIT`, or a mark range ending inside it)
-- must take its chunks written so far back out of the cache: they cannot serve any read.

SET allow_experimental_parquet_parts = 1;
SET max_threads = 1;
SET enable_parallel_replicas = 0;
SET use_indexes_refiner_in_read_pools = 0;

DROP TABLE IF EXISTS t_parquet_cc_withdraw;

CREATE TABLE t_parquet_cc_withdraw (id UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_parquet_part = 1, parquet_row_group_granularity_rows = 200000, parquet_row_group_granularity_bytes = 1000000000;

INSERT INTO t_parquet_cc_withdraw SELECT number, toString(number) FROM numbers(400000)
SETTINGS max_block_size = 1000000, min_insert_block_size_rows = 1000000;

SELECT part_type, marks FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_cc_withdraw' AND active;

SYSTEM DROP COLUMNS CACHE;
SELECT id FROM t_parquet_cc_withdraw LIMIT 1 SETTINGS use_columns_cache = 1, max_block_size = 1;
SELECT 'limit', count() FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_parquet_cc_withdraw';

-- A read of the whole table writes the complete row groups.
SYSTEM DROP COLUMNS CACHE;
SELECT sum(id) FROM t_parquet_cc_withdraw SETTINGS use_columns_cache = 1;
SELECT 'full', column, count(), sum(rows) FROM system.columns_cache
WHERE database = currentDatabase() AND table = 't_parquet_cc_withdraw' GROUP BY column ORDER BY column;

DROP TABLE t_parquet_cc_withdraw;
