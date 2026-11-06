-- `optimize_aggregation_max_by_sorting_key` with a filter on a `Nullable` primary key column after the aggregated
-- column: the rows at the granule boundaries are checked against the filter with their values from the primary index,
-- where a NULL would pass `isNotNull`. Such key columns are not supported with a filter that needs this check.
-- `granules` prints the `MaxBySortingKey` step of `EXPLAIN indexes = 1`, or 'not applied'.

SET enable_parallel_replicas = 0;
-- One part per `INSERT`, so that the number of granules is deterministic.
SET max_block_size = 65409, max_insert_block_size = 1048449, min_insert_block_size_rows = 1048449, min_insert_block_size_bytes = 0;

DROP TABLE IF EXISTS t;
CREATE TABLE t (k UInt8, n Nullable(UInt32)) ENGINE = MergeTree ORDER BY (k, n)
    SETTINGS allow_nullable_key = 1, index_granularity = 4, index_granularity_bytes = '10Mi';
-- 1 run of 12 rows: 2 granules with n = 1, then 1 granule of NULLs.
INSERT INTO t SELECT 1, if(number < 8, 1, NULL) FROM numbers(12);

SELECT '-- uniqExact of the prefix';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT uniqExact(k) FROM t WHERE isNotNull(n) SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT uniqExact(k) FROM t WHERE isNotNull(n) SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT uniqExact(k) FROM t WHERE isNotNull(n) SETTINGS optimize_aggregation_max_by_sorting_key = 1;

DROP TABLE t;

CREATE TABLE t (k UInt8, m UInt32, n Nullable(UInt32)) ENGINE = MergeTree ORDER BY (k, m, n)
    SETTINGS allow_nullable_key = 1, index_granularity = 4, index_granularity_bytes = '10Mi';
-- 1 run of k with 12 rows; n is NULL in the last granule.
INSERT INTO t SELECT 1, number + 1, if(number < 8, 1, NULL) FROM numbers(12);

SELECT '-- max, min and max, DISTINCT, LIMIT BY';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, max(m) FROM t WHERE isNotNull(n) GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, max(m) FROM t WHERE isNotNull(n) GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, max(m) FROM t WHERE isNotNull(n) GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;
SELECT k, min(m), max(m) FROM t WHERE isNotNull(n) GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, min(m), max(m) FROM t WHERE isNotNull(n) GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;
SELECT DISTINCT k FROM t WHERE isNotNull(n) SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT DISTINCT k FROM t WHERE isNotNull(n) SETTINGS optimize_aggregation_max_by_sorting_key = 1;
SELECT k, m FROM t WHERE isNotNull(n) ORDER BY k, m DESC LIMIT 2 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 0;
SELECT k, m FROM t WHERE isNotNull(n) ORDER BY k, m DESC LIMIT 2 BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;

SELECT '-- without a filter on n, the optimization still applies';
SELECT if(has(lines, 'MaxBySortingKey'), lines[indexOf(lines, 'MaxBySortingKey') + 3], 'not applied') AS granules
FROM (SELECT groupArray(trimLeft(explain)) AS lines FROM (EXPLAIN indexes = 1 SELECT k, max(m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1));
SELECT k, max(m) FROM t GROUP BY k SETTINGS optimize_aggregation_max_by_sorting_key = 1;

DROP TABLE t;
