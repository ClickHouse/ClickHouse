-- `optimize_aggregation_max_by_sorting_key` with a column added after the insert, whose default expression uses a
-- stateful function: the reader computes it for the parts that do not have it, so skipping granules would change its
-- values for the kept rows too. The optimization is not applied with any `DEFAULT` or `MATERIALIZED` expression that
-- calls a function; a constant default does not matter.
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
ALTER TABLE t ADD COLUMN x UInt64 DEFAULT rowNumberInAllBlocks();

SELECT '-- argMax of the column';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, argMax(x, m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, argMax(x, m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, argMax(x, m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;

SELECT '-- LIMIT BY';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, m, x FROM t ORDER BY k, m DESC LIMIT 1 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, m, x FROM t ORDER BY k, m DESC LIMIT 1 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, m, x FROM t ORDER BY k, m DESC LIMIT 1 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;

DROP TABLE t;

SELECT '-- any default that calls a function: not applied';
CREATE TABLE t (k UInt8, m UInt32) ENGINE = MergeTree ORDER BY (k, m)
    SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi';
INSERT INTO t SELECT 1, number FROM numbers(12);
ALTER TABLE t ADD COLUMN x UInt64 DEFAULT m * 2;
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, argMax(x, m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, argMax(x, m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;

DROP TABLE t;

SELECT '-- a constant default: the optimization still applies';
CREATE TABLE t (k UInt8, m UInt32) ENGINE = MergeTree ORDER BY (k, m)
    SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi';
INSERT INTO t SELECT 1, number FROM numbers(12);
ALTER TABLE t ADD COLUMN x UInt64 DEFAULT 7;
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, argMax(x, m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, argMax(x, m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, argMax(x, m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;

DROP TABLE t;
