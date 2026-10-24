-- Tags: no-ordinary-database, no-async-insert, no-fasttest, no-object-storage, no-s3-storage
-- `optimize_aggregation_max_by_sorting_key` on a table with `UNIQUE KEY`: overwritten rows are hidden by a delete
-- bitmap, so an index row may be a hidden row. The optimization is not applied.
-- Here the last granule of the run `k = 1` is overwritten with `k = 2`, so its rows are hidden in the first part.
-- `granules` prints the `MaxBySortingKey` step of `EXPLAIN indexes = 1`, or 'not applied'.

SET enable_unique_key = 1;
SET enable_parallel_replicas = 0;
-- One part per `INSERT`, so that the number of granules is deterministic.
SET max_block_size = 65409, max_insert_block_size = 1048449, min_insert_block_size_rows = 1048449, min_insert_block_size_bytes = 0;

DROP TABLE IF EXISTS t;
CREATE TABLE t (id UInt32, k UInt32, m UInt32) ENGINE = MergeTree ORDER BY (k, m) UNIQUE KEY (id)
    SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi';
SYSTEM STOP MERGES t;
INSERT INTO t SELECT number, 1, number FROM numbers(12);
INSERT INTO t SELECT number, 2, number FROM numbers(8, 4);

SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, max(m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, max(m) FROM t GROUP BY k ORDER BY k SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, max(m) FROM t GROUP BY k ORDER BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;
SELECT DISTINCT k FROM t ORDER BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;
SELECT k, m FROM t ORDER BY k, m DESC LIMIT 1 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;

DROP TABLE t;
