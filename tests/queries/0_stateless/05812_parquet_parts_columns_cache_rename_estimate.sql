-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database, no-fasttest
-- Tag no-parallel: drops the columns cache.
-- Tag no-fasttest: Depends on Parquet

-- With a pending `RENAME COLUMN a TO b`, the reader of a `Parquet` part reads and caches the column `a`
-- stored in the part for `b`, so the columns cache write estimate must charge it.

SET allow_experimental_parquet_parts = 1;
SET max_threads = 1;
SET enable_parallel_replicas = 0;
SET use_indexes_refiner_in_read_pools = 0;

DROP TABLE IF EXISTS t_parquet_cc_rename;

CREATE TABLE t_parquet_cc_rename (id UInt64, a String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_parquet_part = 1;

INSERT INTO t_parquet_cc_rename SELECT number, randomPrintableASCII(1024) FROM numbers(1000);

SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_cc_rename' AND active;

-- The estimate budget is far below the size of `a`.
SYSTEM DROP COLUMNS CACHE;
SELECT sum(length(a)) FROM t_parquet_cc_rename
SETTINGS use_columns_cache = 1, columns_cache_max_estimated_bytes_to_write_to_cache = 100000, columns_cache_max_bytes_to_write_to_cache = 1000000000;
SELECT 'before rename', count() FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_parquet_cc_rename';

-- The rename stays pending, so the part still stores the column as `a`.
SYSTEM STOP MERGES t_parquet_cc_rename;
ALTER TABLE t_parquet_cc_rename RENAME COLUMN a TO b SETTINGS mutations_sync = 0, alter_sync = 0;

SYSTEM DROP COLUMNS CACHE;
SELECT sum(length(b)) FROM t_parquet_cc_rename
SETTINGS use_columns_cache = 1, columns_cache_max_estimated_bytes_to_write_to_cache = 100000, columns_cache_max_bytes_to_write_to_cache = 1000000000;
SELECT 'pending rename', count() FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_parquet_cc_rename';

-- Without the estimate budget the renamed column is written to the cache.
SYSTEM DROP COLUMNS CACHE;
SELECT sum(length(b)) FROM t_parquet_cc_rename SETTINGS use_columns_cache = 1;
SELECT 'pending rename without budget', count() > 0 FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_parquet_cc_rename';

DROP TABLE t_parquet_cc_rename;
