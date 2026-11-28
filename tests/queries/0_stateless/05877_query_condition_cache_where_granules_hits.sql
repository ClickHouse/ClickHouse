-- Tags: no-parallel
-- Tag no-parallel: checks the entries of the instance-wide query condition cache, which other tests clear.

-- A filter in WHERE over a MergeTree read records the granules without matches of a chunk it passes only partially,
-- so the second run of a query reads only the granules with a match.

SET use_query_condition_cache = 1, optimize_move_to_prewhere = 0;
SET max_threads = 1, max_insert_threads = 1, max_block_size = 65409, preferred_block_size_bytes = 0;
SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS t_dense;
DROP TABLE IF EXISTS t_sparse;
DROP TABLE IF EXISTS t_adaptive;

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

SELECT count() FROM t_dense WHERE b = 1 SETTINGS log_comment = '05877_dense_1';
SELECT count() FROM t_dense WHERE b = 1 SETTINGS log_comment = '05877_dense_2';
SELECT count() FROM t_sparse WHERE b = 1 SETTINGS log_comment = '05877_sparse_1';
SELECT count() FROM t_sparse WHERE b = 1 SETTINGS log_comment = '05877_sparse_2';
SELECT count() FROM t_adaptive WHERE s LIKE '%2999%' SETTINGS log_comment = '05877_adaptive_dense_1';
SELECT count() FROM t_adaptive WHERE s LIKE '%2999%' SETTINGS log_comment = '05877_adaptive_dense_2';
SELECT count() FROM t_adaptive WHERE b = 1 SETTINGS log_comment = '05877_adaptive_sparse_1';
SELECT count() FROM t_adaptive WHERE b = 1 SETTINGS log_comment = '05877_adaptive_sparse_2';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['SelectedMarks']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05877_dense_1', '05877_dense_2', '05877_sparse_1', '05877_sparse_2')
    AND event_date >= yesterday()
ORDER BY log_comment;

-- The first run reads all granules, the second one only the granules with a match.
WITH
    (SELECT count() FROM mergeTreeIndex(currentDatabase(), t_adaptive) WHERE rows_in_granule > 0) AS granules,
    (
        SELECT uniqExact(mark_number)
        FROM (SELECT _part_offset AS o FROM t_adaptive WHERE b = 1) AS m
        CROSS JOIN
        (
            SELECT mark_number, sum(rows_in_granule) OVER (ORDER BY mark_number) - rows_in_granule AS start, rows_in_granule AS n
            FROM mergeTreeIndex(currentDatabase(), t_adaptive)
        ) AS g
        WHERE o >= start AND o < start + n
    ) AS granules_with_match
SELECT log_comment, ProfileEvents['SelectedMarks'] = if(log_comment LIKE '%\_1', granules, granules_with_match)
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05877\_adaptive\_%'
    AND event_date >= yesterday()
ORDER BY log_comment;

DROP TABLE t_dense;
DROP TABLE t_sparse;
DROP TABLE t_adaptive;
