-- Tags: no-object-storage
-- On remote disks without a filesystem cache, gaps of one granule are read through, which changes the granule counts.
-- `optimize_aggregation_max_by_sorting_key` with `argMin` and `argMax` of a `Nullable` value: they ignore the rows where
-- the value is NULL, so the rows at the granule boundaries do not prove anything, and the optimization is not applied.
-- The only non-NULL value is in the middle granule of the run, which the optimization would skip.
-- `granules` prints the `MaxBySortingKey` step of `EXPLAIN indexes = 1`, or 'not applied'.

SET enable_parallel_replicas = 0;
-- One part per `INSERT`, so that the number of granules is deterministic.
SET max_block_size = 65409, max_insert_block_size = 1048449, min_insert_block_size_rows = 1048449, min_insert_block_size_bytes = 0;

DROP TABLE IF EXISTS t;
CREATE TABLE t (k UInt8, m UInt32, x Nullable(UInt32), s LowCardinality(Nullable(String)), y UInt32) ENGINE = MergeTree ORDER BY (k, m)
    SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi';
-- 1 run of 12 rows: 3 granules.
INSERT INTO t SELECT 1, number + 1, if(number = 4, 42, NULL), if(number = 4, 'a', NULL), if(number = 4, 42, 0) FROM numbers(12);

SELECT '-- Nullable column';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, argMin(x, m), argMax(x, m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, argMin(x, m), argMax(x, m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, argMin(x, m), argMax(x, m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;
SELECT k, argMax(x, m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;

SELECT '-- LowCardinality(Nullable) column';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, argMax(s, m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, argMin(s, m), argMax(s, m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, argMin(s, m), argMax(s, m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;

SELECT '-- Nullable expression';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, argMax(nullIf(y, 0), m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, argMin(nullIf(y, 0), m), argMax(nullIf(y, 0), m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, argMin(nullIf(y, 0), m), argMax(nullIf(y, 0), m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;

SELECT '-- not Nullable: applied, the first and the last granule are read';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, argMin(y, m), argMax(y, m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, argMin(y, m), argMax(y, m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, argMin(y, m), argMax(y, m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;

DROP TABLE t;
