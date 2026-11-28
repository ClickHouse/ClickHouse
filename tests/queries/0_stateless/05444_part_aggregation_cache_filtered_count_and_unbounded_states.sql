-- Tags: no-parallel, no-parallel-replicas
-- Tag no-parallel: the part aggregation cache is global and the test checks its size
-- Tag no-parallel-replicas: the part aggregation cache is intentionally not used for parallel-replica reads

SET allow_experimental_part_aggregation_cache = 1, optimize_aggregation_in_order = 0, enable_memory_bound_merging_of_aggregation_results = 0, max_rows_to_group_by = 0, max_rows_to_read = 0, max_bytes_to_read = 0, max_rows_to_read_leaf = 0, max_bytes_to_read_leaf = 0;

SYSTEM DROP PART AGGREGATION CACHE;

DROP TABLE IF EXISTS t_pac_filtered_count;
CREATE TABLE t_pac_filtered_count (f UInt8, v UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_pac_filtered_count SELECT number % 2, number FROM numbers(32);

-- A filter removes the last column read for a keyless `count`; the row count must survive it.
-- Each query runs twice: the first populates the cache, the second reads from it.
SELECT count() FROM t_pac_filtered_count PREWHERE f;
SELECT count() FROM t_pac_filtered_count PREWHERE f;
SELECT count() FROM t_pac_filtered_count WHERE v % 2 = 0 SETTINGS optimize_move_to_prewhere = 0;
SELECT count() FROM t_pac_filtered_count WHERE v % 2 = 0 SETTINGS optimize_move_to_prewhere = 0;
SELECT count() > 0 FROM system.part_aggregation_cache;

-- States owning memory outside the aggregation arenas (`uniqExact`, `quantile`) are not cached:
-- the cache budget cannot account for them, and `uniqExact` may modify its merged state in place.
SYSTEM DROP PART AGGREGATION CACHE;
SELECT uniqExact(v), quantileExact(v) FROM t_pac_filtered_count;
SELECT count() FROM system.part_aggregation_cache;

-- Settings captured by functions inside a lambda are not part of the cache key, so expressions
-- with lambdas are not cached.
DROP TABLE IF EXISTS t_pac_lambda;
CREATE TABLE t_pac_lambda (arr Array(String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_pac_lambda VALUES (['01/02/2024']);
SELECT sum(arraySum(arrayMap(x -> toUInt64(CAST(x AS DateTime('UTC'))), arr))) FROM t_pac_lambda SETTINGS cast_string_to_date_time_mode = 'best_effort';
SELECT sum(arraySum(arrayMap(x -> toUInt64(CAST(x AS DateTime('UTC'))), arr))) FROM t_pac_lambda SETTINGS cast_string_to_date_time_mode = 'best_effort_us';
SELECT count() FROM system.part_aggregation_cache;

DROP TABLE t_pac_filtered_count;
DROP TABLE t_pac_lambda;
SYSTEM DROP PART AGGREGATION CACHE;
