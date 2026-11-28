-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database, no-fasttest
-- Tag no-parallel: drops the columns cache.
-- Tag no-fasttest: Depends on Parquet

-- A member of `Nested` missing in a `Parquet` part takes its offsets from an existing sibling, which the reader
-- reads and caches as a whole, so the columns cache write estimate must charge that sibling.

SET allow_experimental_parquet_parts = 1;
SET max_threads = 1;
SET enable_parallel_replicas = 0;
SET use_indexes_refiner_in_read_pools = 0;

DROP TABLE IF EXISTS t_parquet_cc_missing_nested;

CREATE TABLE t_parquet_cc_missing_nested (id UInt64, `n.a` Array(String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_parquet_part = 1;

INSERT INTO t_parquet_cc_missing_nested SELECT number, [randomPrintableASCII(1024)] FROM numbers(1000);

ALTER TABLE t_parquet_cc_missing_nested ADD COLUMN `n.b` Array(UInt64);

SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_cc_missing_nested' AND active;

-- The estimate budget is far above the size of `id`, but far below the size of `n.a`.
SYSTEM DROP COLUMNS CACHE;
SELECT sum(id), sum(length(n.b)) FROM t_parquet_cc_missing_nested
SETTINGS use_columns_cache = 1, columns_cache_max_estimated_bytes_to_write_to_cache = 100000, columns_cache_max_bytes_to_write_to_cache = 1000000000;
SELECT 'missing member', count() FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_parquet_cc_missing_nested';

SYSTEM DROP COLUMNS CACHE;
SELECT sum(n.b.size0) FROM t_parquet_cc_missing_nested
SETTINGS use_columns_cache = 1, columns_cache_max_estimated_bytes_to_write_to_cache = 100000, columns_cache_max_bytes_to_write_to_cache = 1000000000;
SELECT 'missing member offsets', count() FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_parquet_cc_missing_nested';

-- Without the estimate budget the same query writes the sibling to the cache.
SYSTEM DROP COLUMNS CACHE;
SELECT sum(id), sum(length(n.b)) FROM t_parquet_cc_missing_nested SETTINGS use_columns_cache = 1;
SELECT 'missing member without budget', column, count() > 0 FROM system.columns_cache
WHERE database = currentDatabase() AND table = 't_parquet_cc_missing_nested' GROUP BY column ORDER BY column;

DROP TABLE t_parquet_cc_missing_nested;
