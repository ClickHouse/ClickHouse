-- `optimize_aggregation_max_by_sorting_key` with a stateful function (`rowNumberInAllBlocks`) evaluated before the
-- aggregation or the `LIMIT BY`: its value depends on all rows it processes, so skipping granules would change it for
-- the kept rows too. The optimization is not applied.
-- `granules` prints the `MaxBySortingKey` step of `EXPLAIN indexes = 1`, or 'not applied'.

SET enable_parallel_replicas = 0;
-- One stream reading the part in order, so that the row numbers are deterministic.
SET max_threads = 1, optimize_read_in_order = 0;
-- One part per `INSERT`, so that the number of granules is deterministic.
SET max_block_size = 65409, max_insert_block_size = 1048449, min_insert_block_size_rows = 1048449, min_insert_block_size_bytes = 0;

DROP TABLE IF EXISTS t;
CREATE TABLE t (k UInt8, m UInt32) ENGINE = MergeTree ORDER BY (k, m)
    SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi';
-- 1 run of 12 rows: 3 granules.
INSERT INTO t SELECT 1, number FROM numbers(12);

SELECT '-- argMax and argMin of rowNumberInAllBlocks';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, argMax(rowNumberInAllBlocks(), m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, argMax(rowNumberInAllBlocks(), m), argMin(rowNumberInAllBlocks(), m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, argMax(rowNumberInAllBlocks(), m), argMin(rowNumberInAllBlocks(), m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;

SELECT '-- LIMIT BY, the function evaluated before the sorting';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, m, rowNumberInAllBlocks() FROM t ORDER BY k, m DESC LIMIT 1 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1, query_plan_execute_functions_after_sorting = 0));
SELECT k, m, rowNumberInAllBlocks() FROM t ORDER BY k, m DESC LIMIT 1 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 0, query_plan_execute_functions_after_sorting = 0;
SELECT k, m, rowNumberInAllBlocks() FROM t ORDER BY k, m DESC LIMIT 1 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1, query_plan_execute_functions_after_sorting = 0;

SELECT '-- LIMIT BY, the function evaluated after the sorting';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, m, rowNumberInAllBlocks() FROM t ORDER BY k, m DESC LIMIT 1 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1, query_plan_execute_functions_after_sorting = 1));
SELECT k, m, rowNumberInAllBlocks() FROM t ORDER BY k, m DESC LIMIT 1 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 0, query_plan_execute_functions_after_sorting = 1;
SELECT k, m, rowNumberInAllBlocks() FROM t ORDER BY k, m DESC LIMIT 1 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1, query_plan_execute_functions_after_sorting = 1;

SELECT '-- without a stateful function, the optimization still applies';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, argMax(m * 2, m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, argMax(m * 2, m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;

DROP TABLE t;
