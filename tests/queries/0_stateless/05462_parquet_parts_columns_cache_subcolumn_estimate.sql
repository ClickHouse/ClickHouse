-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database, no-fasttest
-- Tag no-parallel: drops the columns cache.
-- Tag no-fasttest: Depends on Parquet

-- The reader of `Parquet` parts reads and caches the whole column for a subcolumn,
-- so the columns cache write estimate must charge the whole column, not only the subcolumn.

SET max_threads = 1;
SET enable_parallel_replicas = 0;
SET use_indexes_refiner_in_read_pools = 0;

DROP TABLE IF EXISTS t_parquet_cc_subcolumn;

CREATE TABLE t_parquet_cc_subcolumn (id UInt64, tup Tuple(a UInt64, b String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_parquet_part = 1;

INSERT INTO t_parquet_cc_subcolumn SELECT number, (number, randomPrintableASCII(1024)) FROM numbers(1000);

SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_cc_subcolumn' AND active;

-- The estimate budget is far above the size of `tup.a` and of `id`, but far below the size of `tup`.
SYSTEM DROP COLUMNS CACHE;
SELECT sum(tup.a) FROM t_parquet_cc_subcolumn
SETTINGS use_columns_cache = 1, columns_cache_max_estimated_bytes_to_write_to_cache = 100000, columns_cache_max_bytes_to_write_to_cache = 1000000000;
SELECT 'subcolumn', count() FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_parquet_cc_subcolumn';

SYSTEM DROP COLUMNS CACHE;
SELECT sum(length(tup.b)) FROM t_parquet_cc_subcolumn
SETTINGS use_columns_cache = 1, columns_cache_max_estimated_bytes_to_write_to_cache = 100000, columns_cache_max_bytes_to_write_to_cache = 1000000000;
SELECT 'whole column', count() FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_parquet_cc_subcolumn';

-- A small column under the same budget is written to the cache.
SYSTEM DROP COLUMNS CACHE;
SELECT sum(id) FROM t_parquet_cc_subcolumn
SETTINGS use_columns_cache = 1, columns_cache_max_estimated_bytes_to_write_to_cache = 100000, columns_cache_max_bytes_to_write_to_cache = 1000000000;
SELECT 'small column', count() > 0 FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_parquet_cc_subcolumn';

-- Without the estimate budget the subcolumn query writes to the cache.
SYSTEM DROP COLUMNS CACHE;
SELECT sum(tup.a) FROM t_parquet_cc_subcolumn SETTINGS use_columns_cache = 1;
SELECT 'subcolumn without budget', count() > 0 FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_parquet_cc_subcolumn';

DROP TABLE t_parquet_cc_subcolumn;
