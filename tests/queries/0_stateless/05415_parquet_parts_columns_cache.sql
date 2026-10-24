-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database, no-fasttest
-- Tag no-parallel: drops the columns cache.
-- Tag no-fasttest: Depends on Parquet

-- The columns cache serves the columns of data parts in Parquet format: the columns of row groups are cached (in chunks of up to 65536 rows).

SET max_threads = 1;
-- `enable_parallel_replicas = 0`: with parallel replicas another replica may do some of the reads,
-- so the entries written by this query would depend on how the reads are split.
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_parquet_columns_cache;

CREATE TABLE t_parquet_columns_cache (id UInt64, value String, v Variant(UInt64, String))
ENGINE = MergeTree ORDER BY id
SETTINGS min_rows_for_parquet_part = 1, parquet_row_group_granularity_rows = 10000;

INSERT INTO t_parquet_columns_cache SELECT number, toString(number), if(number % 2, number::Variant(UInt64, String), toString(number)::Variant(UInt64, String)) FROM numbers(100000);

SYSTEM DROP COLUMNS CACHE;

SELECT part_type, marks FROM system.parts WHERE database = currentDatabase() AND table = 't_parquet_columns_cache' AND active;

SELECT 'without cache', sum(id), sum(length(value)), countIf(v.UInt64 IS NOT NULL) FROM t_parquet_columns_cache SETTINGS use_columns_cache = 0, log_comment = 'pq05415_no_cache';
SELECT 'populate', sum(id), sum(length(value)), countIf(v.UInt64 IS NOT NULL) FROM t_parquet_columns_cache SETTINGS use_columns_cache = 1, log_comment = 'pq05415_populate';

SELECT 'entries';
SELECT column, count(), sum(rows), min(rows), max(rows) FROM system.columns_cache
WHERE database = currentDatabase() AND table = 't_parquet_columns_cache' GROUP BY column ORDER BY column;

SELECT 'from cache', sum(id), sum(length(value)), countIf(v.UInt64 IS NOT NULL) FROM t_parquet_columns_cache SETTINGS use_columns_cache = 1, log_comment = 'pq05415_hit';
SELECT 'from cache with filter', sum(id), count() FROM t_parquet_columns_cache WHERE id % 7 = 3 SETTINGS use_columns_cache = 1, log_comment = 'pq05415_hit_filter';
SELECT 'from cache, part of a row group', sum(id), count() FROM t_parquet_columns_cache WHERE id BETWEEN 25000 AND 25009 SETTINGS use_columns_cache = 1, max_block_size = 3;

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['ColumnsCacheHits'] > 0, ProfileEvents['ColumnsCacheMisses'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'pq05415%'
ORDER BY log_comment;

SELECT 'dropped with the table';
DROP TABLE t_parquet_columns_cache;
SELECT count() FROM system.columns_cache WHERE database = currentDatabase() AND table = 't_parquet_columns_cache';
