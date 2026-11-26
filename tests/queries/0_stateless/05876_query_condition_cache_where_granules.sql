-- A filter in WHERE over a MergeTree read records in the query condition cache the granules without matches,
-- also of a chunk it passes only partially. Each query runs twice (the second one reads the entries of the first),
-- then without the cache. Each case uses its own table, so it reads only its own entries.

SET use_query_condition_cache = 1, optimize_move_to_prewhere = 0;
SET max_threads = 1, max_insert_threads = 1, max_block_size = 65409, preferred_block_size_bytes = 0;
SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS t_dense;
DROP TABLE IF EXISTS t_sparse;
DROP TABLE IF EXISTS t_reverse;
DROP TABLE IF EXISTS t_array;
DROP TABLE IF EXISTS t_array_join;
DROP TABLE IF EXISTS t_prewhere;
DROP TABLE IF EXISTS t_delete;
DROP TABLE IF EXISTS t_adaptive;
DROP TABLE IF EXISTS t_and_chain;
DROP TABLE IF EXISTS t_top_k;

-- 100 granules of 1000 rows and one of 500 rows, b = 1 in 10 rows of the granules 1, 50 and 100.
CREATE TABLE t_dense (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 1000, index_granularity_bytes = 0, add_minmax_index_for_numeric_columns = 0,
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1;
INSERT INTO t_dense SELECT number, (number BETWEEN 1000 AND 1009) OR (number BETWEEN 50000 AND 50009) OR (number BETWEEN 100490 AND 100499)
FROM numbers(100500);

CREATE TABLE t_sparse (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 1000, index_granularity_bytes = 0, add_minmax_index_for_numeric_columns = 0,
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 0.5;
INSERT INTO t_sparse SELECT * FROM t_dense;

SELECT '-- dense';
SELECT count() FROM t_dense WHERE b = 1;
SELECT count() FROM t_dense WHERE b = 1;
SELECT count() FROM t_dense WHERE b = 1 SETTINGS use_query_condition_cache = 0;

SELECT '-- sparse';
SELECT serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_sparse' AND column = 'b' AND active;
SELECT count() FROM t_sparse WHERE b = 1;
SELECT count() FROM t_sparse WHERE b = 1;
SELECT count() FROM t_sparse WHERE b = 1 SETTINGS use_query_condition_cache = 0;

SELECT '-- reading in reverse order';
CREATE TABLE t_reverse AS t_dense;
INSERT INTO t_reverse SELECT * FROM t_dense;
SELECT groupArray(a) FROM (SELECT a FROM t_reverse WHERE b = 1 ORDER BY a DESC) SETTINGS optimize_read_in_order = 1;
SELECT groupArray(a) FROM (SELECT a FROM t_reverse WHERE b = 1 ORDER BY a DESC) SETTINGS optimize_read_in_order = 1;
SELECT groupArray(a) FROM (SELECT a FROM t_reverse WHERE b = 1 ORDER BY a DESC) SETTINGS optimize_read_in_order = 1, use_query_condition_cache = 0;
SELECT count() FROM t_reverse WHERE b = 1;

SELECT '-- ARRAY JOIN';
CREATE TABLE t_array (a UInt64, b UInt64, arr Array(UInt64))
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 1000, index_granularity_bytes = 0, add_minmax_index_for_numeric_columns = 0,
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1;
INSERT INTO t_array SELECT a, b, multiIf(a < 1000, [a, a], a < 2000, [], [a]) FROM t_dense;
SELECT count() FROM t_array ARRAY JOIN arr AS x WHERE x = 999 SETTINGS query_plan_fuse_filter_into_array_join = 0;
SELECT count() FROM t_array ARRAY JOIN arr AS x WHERE x = 999 SETTINGS query_plan_fuse_filter_into_array_join = 0;
SELECT count() FROM t_array ARRAY JOIN arr AS x WHERE x = 999 SETTINGS query_plan_fuse_filter_into_array_join = 0, use_query_condition_cache = 0;
SELECT count() FROM t_array WHERE arrayJoin(arr) = 999;
SELECT count() FROM t_array WHERE arrayJoin(arr) = 999;
SELECT count() FROM t_array WHERE arrayJoin(arr) = 999 SETTINGS use_query_condition_cache = 0;
-- b = 1 is evaluated below the ARRAY JOIN, x = 1 above it.
CREATE TABLE t_array_join AS t_dense;
INSERT INTO t_array_join SELECT * FROM t_dense;
SELECT count() FROM t_array_join ARRAY JOIN range(a % 3) AS x WHERE b = 1 AND x = 1;
SELECT count() FROM t_array_join ARRAY JOIN range(a % 3) AS x WHERE b = 1 AND x = 1;
SELECT count() FROM t_array_join ARRAY JOIN range(a % 3) AS x WHERE b = 1 AND x = 1 SETTINGS use_query_condition_cache = 0;
SELECT count() FROM t_array_join ARRAY JOIN range(a % 3) AS x WHERE b = 1;
SELECT count() FROM t_array_join WHERE b = 1;

SELECT '-- PREWHERE and WHERE';
-- PREWHERE removes the first 10 rows of granule 0, so the rows of the next granules are not at their positions.
CREATE TABLE t_prewhere AS t_dense;
INSERT INTO t_prewhere SELECT * FROM t_dense;
SELECT count() FROM t_prewhere PREWHERE a >= 10 WHERE b = 1;
SELECT count() FROM t_prewhere PREWHERE a >= 10 WHERE b = 1;
SELECT count() FROM t_prewhere PREWHERE a >= 10 WHERE b = 1 SETTINGS use_query_condition_cache = 0;
SELECT count() FROM t_prewhere WHERE b = 1;

SELECT '-- lightweight delete';
CREATE TABLE t_delete AS t_sparse;
INSERT INTO t_delete SELECT * FROM t_sparse;
DELETE FROM t_delete WHERE a < 10 SETTINGS lightweight_deletes_sync = 2;
SELECT count() FROM t_delete WHERE b = 1;
SELECT count() FROM t_delete WHERE b = 1;
SELECT count() FROM t_delete WHERE b = 1 SETTINGS use_query_condition_cache = 0;
SELECT count() FROM t_delete WHERE b = 1 SETTINGS apply_deleted_mask = 0;
SELECT count() FROM t_delete WHERE b = 1 SETTINGS apply_deleted_mask = 0;

SELECT '-- granules of different sizes';
-- Adaptive granularity sizes the granules by the size of the rows: one part from parts with rows of different sizes.
CREATE TABLE t_adaptive (a UInt64, b UInt64, s String)
ENGINE = MergeTree ORDER BY a
SETTINGS index_granularity = 1000, index_granularity_bytes = 20000, use_const_adaptive_granularity = 0,
         add_minmax_index_for_numeric_columns = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         ratio_of_defaults_for_sparse_serialization = 0.5;
INSERT INTO t_adaptive SELECT number, position(s, '2999') > 0, s FROM (SELECT number, repeat('x', 10) || toString(number) AS s FROM numbers(0, 30000));
INSERT INTO t_adaptive SELECT number, position(s, '2999') > 0, s FROM (SELECT number, repeat('x', 90) || toString(number) AS s FROM numbers(30000, 30000));
INSERT INTO t_adaptive SELECT number, position(s, '2999') > 0, s FROM (SELECT number, repeat('x', 40) || toString(number) AS s FROM numbers(60000, 40000));
OPTIMIZE TABLE t_adaptive FINAL;
SELECT uniqExact(part_name) = 1, uniqExact(rows_in_granule) >= 3 FROM mergeTreeIndex(currentDatabase(), t_adaptive) WHERE rows_in_granule > 0;
SELECT serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_adaptive' AND column = 'b' AND active;
SELECT count(), sum(a) FROM t_adaptive WHERE s LIKE '%2999%';
SELECT count(), sum(a) FROM t_adaptive WHERE s LIKE '%2999%';
SELECT count(), sum(a) FROM t_adaptive WHERE s LIKE '%2999%' SETTINGS use_query_condition_cache = 0;
SELECT count(), sum(a) FROM t_adaptive WHERE b = 1;
SELECT count(), sum(a) FROM t_adaptive WHERE b = 1;
SELECT count(), sum(a) FROM t_adaptive WHERE b = 1 SETTINGS use_query_condition_cache = 0;

SELECT '-- the split filters of an AND chain';
-- Without a primary key, both filters see all granules, and the first one removes rows of the chunk.
CREATE TABLE t_and_chain (a UInt64, b UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 1000, index_granularity_bytes = 0, add_minmax_index_for_numeric_columns = 0,
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 1;
INSERT INTO t_and_chain SELECT * FROM t_dense ORDER BY a;
SELECT count() FROM t_and_chain WHERE a >= 1005 AND b = 1 SETTINGS query_plan_merge_filters = 1;
SELECT count() FROM t_and_chain WHERE a >= 1005 AND b = 1 SETTINGS query_plan_merge_filters = 1;
SELECT count() FROM t_and_chain WHERE a >= 1005 AND b = 1 SETTINGS query_plan_merge_filters = 1, use_query_condition_cache = 0;

SELECT '-- ORDER BY ... LIMIT';
CREATE TABLE t_top_k AS t_dense;
INSERT INTO t_top_k SELECT * FROM t_dense;
SELECT groupArray(a) FROM (SELECT a FROM t_top_k WHERE b = 1 ORDER BY a LIMIT 5) SETTINGS use_top_k_dynamic_filtering = 1, use_query_condition_cache_for_top_k = 1;
SELECT groupArray(a) FROM (SELECT a FROM t_top_k WHERE b = 1 ORDER BY a LIMIT 5) SETTINGS use_top_k_dynamic_filtering = 1, use_query_condition_cache_for_top_k = 1;
SELECT groupArray(a) FROM (SELECT a FROM t_top_k WHERE b = 1 ORDER BY a LIMIT 5) SETTINGS use_top_k_dynamic_filtering = 1, use_query_condition_cache = 0;

DROP TABLE t_dense;
DROP TABLE t_sparse;
DROP TABLE t_reverse;
DROP TABLE t_array;
DROP TABLE t_array_join;
DROP TABLE t_prewhere;
DROP TABLE t_delete;
DROP TABLE t_adaptive;
DROP TABLE t_and_chain;
DROP TABLE t_top_k;
