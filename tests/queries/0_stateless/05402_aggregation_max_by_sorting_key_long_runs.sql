-- `optimize_aggregation_max_by_sorting_key` with `ORDER BY <keys>, k LIMIT n BY <keys>` where a run has fewer than `n`
-- rows: nothing can be skipped. The rows of each run are counted once per part, not once per granule, so planning
-- stays fast with long runs. `granules` prints the `MaxBySortingKey` step of `EXPLAIN indexes = 1`, or 'not applied'.

SET enable_parallel_replicas = 0;
-- One part per `INSERT`, so that the number of granules is deterministic.
SET max_block_size = 65409, max_insert_block_size = 1048449, min_insert_block_size_rows = 1048449, min_insert_block_size_bytes = 0;

DROP TABLE IF EXISTS t;
CREATE TABLE t (k String, m UInt32) ENGINE = MergeTree ORDER BY (k, m)
    SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi';
-- 2 runs of 12 rows each: 3 granules per run.
INSERT INTO t SELECT if(number < 12, 'A', 'B'), number % 12 + 1 FROM numbers(24);

SELECT '-- DESC, LIMIT 5: 4 + 1 rows follow the first granule of each run';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, m FROM t ORDER BY k, m DESC LIMIT 5 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, arraySort(groupArray(m)) FROM (SELECT k, m FROM t ORDER BY k, m DESC LIMIT 5 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 0) GROUP BY k ORDER BY k;
SELECT k, arraySort(groupArray(m)) FROM (SELECT k, m FROM t ORDER BY k, m DESC LIMIT 5 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1) GROUP BY k ORDER BY k;

SELECT '-- DESC, LIMIT 20: longer than the runs';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, m FROM t ORDER BY k, m DESC LIMIT 20 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, count(), min(m), max(m) FROM (SELECT k, m FROM t ORDER BY k, m DESC LIMIT 20 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 0) GROUP BY k ORDER BY k;
SELECT k, count(), min(m), max(m) FROM (SELECT k, m FROM t ORDER BY k, m DESC LIMIT 20 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1) GROUP BY k ORDER BY k;

SELECT '-- ASC, LIMIT 4: the second granule of each run follows 4 rows';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, m FROM t ORDER BY k, m LIMIT 4 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, arraySort(groupArray(m)) FROM (SELECT k, m FROM t ORDER BY k, m LIMIT 4 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 0) GROUP BY k ORDER BY k;
SELECT k, arraySort(groupArray(m)) FROM (SELECT k, m FROM t ORDER BY k, m LIMIT 4 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1) GROUP BY k ORDER BY k;

SELECT '-- ASC, LIMIT 20: longer than the runs';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, m FROM t ORDER BY k, m LIMIT 20 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, count(), min(m), max(m) FROM (SELECT k, m FROM t ORDER BY k, m LIMIT 20 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 0) GROUP BY k ORDER BY k;
SELECT k, count(), min(m), max(m) FROM (SELECT k, m FROM t ORDER BY k, m LIMIT 20 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1) GROUP BY k ORDER BY k;

DROP TABLE t;

-- One run of 100000 granules. Counting the rows of the run once per granule would take minutes here. Only the plans are
-- checked: reading every granule of this table is slow, and the results are checked on the small table above.
-- A `Wide` part: a `Compact` part compresses every granule separately, which takes minutes for 100000 granules in
-- debug builds.
DROP TABLE IF EXISTS t_long;
CREATE TABLE t_long (k UInt8, m UInt32) ENGINE = MergeTree ORDER BY (k, m)
    SETTINGS index_granularity = 1, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_long SELECT 1, number FROM numbers(100000);

SELECT '-- long run, LIMIT longer than the run, DESC and ASC';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, m FROM t_long ORDER BY k, m DESC LIMIT 1000000 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, m FROM t_long ORDER BY k, m LIMIT 1000000 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));

SELECT '-- long run, LIMIT 3 DESC: the last 3 granules are read';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, m FROM t_long ORDER BY k, m DESC LIMIT 3 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, m FROM t_long ORDER BY k, m DESC LIMIT 3 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;

DROP TABLE t_long;
