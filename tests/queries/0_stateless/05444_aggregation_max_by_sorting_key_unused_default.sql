-- `optimize_aggregation_max_by_sorting_key` is not applied to tables with a `DEFAULT` or `MATERIALIZED` expression that
-- calls a function, because the reader may compute it while reading: for columns that a part does not have, and when
-- converting a column to a new type. Only the parsed expressions are inspected, so a default whose function cannot be
-- used in the query does not make the query fail.
-- `granules` prints the `MaxBySortingKey` step of `EXPLAIN indexes = 1`, or 'not applied'.

SET enable_parallel_replicas = 0;
-- One stream reading the part in order, so that the row numbers are deterministic.
SET max_threads = 1, optimize_read_in_order = 0;
-- One part per `INSERT`, so that the number of granules is deterministic.
SET max_block_size = 65409, max_insert_block_size = 1048449, min_insert_block_size_rows = 1048449, min_insert_block_size_bytes = 0;

SELECT '-- a stored column with a default using a deprecated function';
SET allow_deprecated_error_prone_window_functions = 1;
DROP TABLE IF EXISTS t;
CREATE TABLE t (k UInt8, m UInt32, x Int64 DEFAULT runningDifference(m)) ENGINE = MergeTree ORDER BY (k, m)
    SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi';
INSERT INTO t (k, m, x) SELECT 1, number, number FROM numbers(12);
SET allow_deprecated_error_prone_window_functions = 0;
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, max(m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, max(m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;
DROP TABLE t;

SELECT '-- a pending rename of a column with a default using an introspection function';
SET allow_introspection_functions = 1;
CREATE TABLE t (k UInt8, m UInt32, x String DEFAULT addressToSymbol(toUInt64(0))) ENGINE = MergeTree ORDER BY (k, m)
    SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi';
INSERT INTO t (k, m, x) SELECT 1, number, toString(number) FROM numbers(12);
SYSTEM STOP MERGES t;
ALTER TABLE t RENAME COLUMN x TO y SETTINGS alter_sync = 0;
SET allow_introspection_functions = 0;
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, argMax(y, m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, argMax(y, m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;
DROP TABLE t;

SELECT '-- a pending conversion from Nullable that fills NULL with a stateful default';
SET apply_mutations_on_fly = 0, apply_patch_parts = 0;
CREATE TABLE t (k UInt8, m UInt32, x Nullable(UInt64)) ENGINE = MergeTree ORDER BY (k, m)
    SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi';
INSERT INTO t SELECT 1, number, NULL FROM numbers(12);
SYSTEM STOP MERGES t;
ALTER TABLE t MODIFY COLUMN x UInt64 DEFAULT rowNumberInAllBlocks() SETTINGS alter_sync = 0;
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, argMax(x, m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, argMax(x, m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, argMax(x, m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;
SELECT k, m, x FROM t ORDER BY k, m DESC LIMIT 1 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, m, x FROM t ORDER BY k, m DESC LIMIT 1 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;
DROP TABLE t;
