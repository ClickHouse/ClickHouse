-- `optimize_aggregation_max_by_sorting_key` only looks at the default expressions that the reader computes: those of
-- the columns read that a part does not have. A default whose function cannot be used in the query (here a deprecated
-- function) does not matter when the parts have the column, or when the query does not read it.
-- `granules` prints the `MaxBySortingKey` step of `EXPLAIN indexes = 1`, or 'not applied'.

SET enable_parallel_replicas = 0;
-- One part per `INSERT`, so that the number of granules is deterministic.
SET max_block_size = 65409, max_insert_block_size = 1048449, min_insert_block_size_rows = 1048449, min_insert_block_size_bytes = 0;

SET allow_deprecated_error_prone_window_functions = 1;
DROP TABLE IF EXISTS t;
CREATE TABLE t (k UInt8, m UInt32, x Int64 DEFAULT runningDifference(m)) ENGINE = MergeTree ORDER BY (k, m)
    SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi';
-- 1 run of 12 rows: 3 granules. The part has the column `x`.
INSERT INTO t (k, m, x) SELECT 1, number, number FROM numbers(12);
SET allow_deprecated_error_prone_window_functions = 0;

SELECT '-- a stored column with a default using a deprecated function';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, max(m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, max(m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, max(m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;
SELECT k, argMax(x, m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;

SELECT '-- a column that the part does not have, with a stateful default, not read';
-- `ALTER` checks every default of the table, also the one with the deprecated function.
SET allow_deprecated_error_prone_window_functions = 1;
ALTER TABLE t ADD COLUMN y UInt64 DEFAULT rowNumberInAllBlocks();
SET allow_deprecated_error_prone_window_functions = 0;
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, max(m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, max(m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;

SELECT '-- the same column, read: not applied';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, max(m), any(y) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));

DROP TABLE t;
