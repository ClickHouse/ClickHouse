-- `optimize_aggregation_max_by_sorting_key` with `ORDER BY <keys>, k LIMIT n OFFSET m BY <keys>` where `n + m` does
-- not fit into `UInt64`: every row of a run may be needed, so nothing is skipped. `LIMIT -n OFFSET m BY` is planned
-- with the largest length and offset `m`. `granules` prints the `MaxBySortingKey` step of `EXPLAIN indexes = 1`, or
-- 'not applied'.

SET enable_parallel_replicas = 0;
-- One part per `INSERT`, so that the number of granules is deterministic.
SET max_block_size = 65409, max_insert_block_size = 1048449, min_insert_block_size_rows = 1048449, min_insert_block_size_bytes = 0;

DROP TABLE IF EXISTS t;
CREATE TABLE t (k UInt8, m UInt32) ENGINE = MergeTree ORDER BY (k, m)
    SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi';
-- 1 run of 12 rows: 3 granules.
INSERT INTO t SELECT 1, number FROM numbers(12);

SELECT '-- negative limit with an offset, DESC';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, m FROM t ORDER BY k, m DESC LIMIT -3 OFFSET 2 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, m FROM t ORDER BY k, m DESC LIMIT -3 OFFSET 2 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, m FROM t ORDER BY k, m DESC LIMIT -3 OFFSET 2 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;

SELECT '-- negative limit with an offset, ASC';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, m FROM t ORDER BY k, m LIMIT -3 OFFSET 2 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, m FROM t ORDER BY k, m LIMIT -3 OFFSET 2 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, m FROM t ORDER BY k, m LIMIT -3 OFFSET 2 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;

SELECT '-- the largest positive limit with an offset, DESC and ASC';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, m FROM t ORDER BY k, m DESC LIMIT 18446744073709551615 OFFSET 2 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT count(), min(m), max(m) FROM (SELECT k, m FROM t ORDER BY k, m DESC LIMIT 18446744073709551615 OFFSET 2 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 0);
SELECT count(), min(m), max(m) FROM (SELECT k, m FROM t ORDER BY k, m DESC LIMIT 18446744073709551615 OFFSET 2 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1);
SELECT count(), min(m), max(m) FROM (SELECT k, m FROM t ORDER BY k, m LIMIT 18446744073709551615 OFFSET 2 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 0);
SELECT count(), min(m), max(m) FROM (SELECT k, m FROM t ORDER BY k, m LIMIT 18446744073709551615 OFFSET 2 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1);

SELECT '-- a limit that fits still applies';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, m FROM t ORDER BY k, m DESC LIMIT 1 OFFSET 2 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, m FROM t ORDER BY k, m DESC LIMIT 1 OFFSET 2 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;

DROP TABLE t;
