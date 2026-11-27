-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database, no-parallel-replicas
-- Tag no-parallel: `SYSTEM DROP COLUMNS CACHE` and the checks of the cache contents are global.

-- A `Nullable` column without NULLs may be read without a null map. A later query reading the same cached
-- column must not create the null map in it, so that the weight of the entry covers the memory it retains.

SET use_columns_cache = 1, enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 1;
SET max_threads = 1, enable_parallel_replicas = 0, compile_expressions = 0;

DROP TABLE IF EXISTS t_cache_nullable;
DROP TABLE IF EXISTS t_cache_bytes;
CREATE TABLE t_cache_nullable (x Nullable(UInt8)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192;
INSERT INTO t_cache_nullable SELECT toUInt8(number % 10) FROM numbers(200000);
CREATE TABLE t_cache_bytes (bytes UInt64) ENGINE = Memory;

SYSTEM DROP COLUMNS CACHE;
SELECT sum(x) FROM t_cache_nullable;
INSERT INTO t_cache_bytes SELECT sum(bytes) FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_cache_nullable';

-- `if` reads the null map of its condition.
SELECT sum(if(x, 1, 0)) FROM t_cache_nullable;

SELECT count() > 0, sum(bytes) = (SELECT bytes FROM t_cache_bytes)
FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_cache_nullable';

DROP TABLE t_cache_nullable;
DROP TABLE t_cache_bytes;
