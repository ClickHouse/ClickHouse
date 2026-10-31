-- `optimize_aggregation_max_by_sorting_key` with `ORDER BY <keys>, k LIMIT n BY <keys>` and `arrayJoin` between the
-- `LIMIT BY` and the sorting (with `query_plan_lower_array_join_function = 0`): a row with an empty array produces no
-- rows, so it does not prove a granule unnecessary. The optimization is not applied.
-- `granules` prints the `MaxBySortingKey` step of `EXPLAIN indexes = 1`, or 'not applied'.

SET enable_parallel_replicas = 0;
SET query_plan_lower_array_join_function = 0;
-- One part per `INSERT`, so that the number of granules is deterministic.
SET max_block_size = 65409, max_insert_block_size = 1048449, min_insert_block_size_rows = 1048449, min_insert_block_size_bytes = 0;

DROP TABLE IF EXISTS t;
CREATE TABLE t (k UInt8, m UInt32, a Array(UInt32)) ENGINE = MergeTree ORDER BY (k, m)
    SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi';
-- 1 run of 12 rows: 3 granules. The only non-empty array is in the middle granule.
INSERT INTO t SELECT 1, number, if(number = 4, [42], []) FROM numbers(12);

SELECT '-- arrayJoin, DESC';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, m, arrayJoin(a) FROM t ORDER BY k, m DESC LIMIT 1 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, m, arrayJoin(a) FROM t ORDER BY k, m DESC LIMIT 1 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, m, arrayJoin(a) FROM t ORDER BY k, m DESC LIMIT 1 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;

SELECT '-- arrayJoin, ASC';
SELECT k, m, arrayJoin(a) FROM t ORDER BY k, m LIMIT 1 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, m, arrayJoin(a) FROM t ORDER BY k, m LIMIT 1 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;

SELECT '-- without arrayJoin, the optimization still applies';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, m, a FROM t ORDER BY k, m DESC LIMIT 1 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, m, a FROM t ORDER BY k, m DESC LIMIT 1 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;

DROP TABLE t;
