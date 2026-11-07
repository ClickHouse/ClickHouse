-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database, no-parallel-replicas
-- Tag no-parallel: `SYSTEM DROP COLUMNS CACHE` and the checks of the cache contents are global.

-- A `Nullable` column without NULLs may be read without a null map, and a later query reading the same cached
-- column may create it lazily. The columns cache must create the null maps before it accounts an entry,
-- so that the weight of the entry covers the memory it retains.

SET use_columns_cache = 1, enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 1;
SET max_threads = 1, enable_parallel_replicas = 0, compile_expressions = 0;

DROP TABLE IF EXISTS t_cache_nullable;
CREATE TABLE t_cache_nullable (x Nullable(UInt8)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192;
INSERT INTO t_cache_nullable SELECT toUInt8(number % 10) FROM numbers(200000);

SYSTEM DROP COLUMNS CACHE;
SELECT sum(x) FROM t_cache_nullable;
SELECT sum(if(x, 1, 0)) FROM t_cache_nullable;

-- One byte per row of the nested column and one of the null map.
SELECT count() > 0, countIf(bytes < 2 * rows) FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_cache_nullable';

DROP TABLE t_cache_nullable;
