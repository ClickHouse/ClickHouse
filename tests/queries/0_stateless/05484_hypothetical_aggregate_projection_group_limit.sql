-- the estimate builds the projection part over all groups of a part, and keeps to the max_rows_to_group_by of the query

SET optimize_use_projections = 1, optimize_use_implicit_projections = 0, prefer_optimize_projection = 0, force_optimize_projection = 0,
    enable_parallel_replicas = 0, optimize_aggregation_in_order = 0;

-- the implicit minmax indices of add_minmax_index_for_numeric_columns would prune the marks of the base table and change
-- the estimates and the real reads this test compares, so the tables opt out
CREATE TABLE t_est (a UInt64, b UInt64, v UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS add_minmax_index_for_numeric_columns = 0, index_granularity = 100, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_est SELECT number, number % 500, number FROM numbers(10000);
CREATE HYPOTHETICAL PROJECTION p ON t_est (SELECT b, sum(v), count() GROUP BY b);

SELECT '-- the query has one group';
SELECT b, sum(v) FROM t_est WHERE b = 1 GROUP BY b SETTINGS max_rows_to_group_by = 10, group_by_overflow_mode = 'throw';

SELECT '-- the projection part has more groups than the limit';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT b, sum(v) FROM t_est WHERE b = 1 GROUP BY b SETTINGS max_rows_to_group_by = 10, group_by_overflow_mode = 'throw')
WHERE match(line, '^(With |empirical_status:|empirical_reason:)');
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT b, sum(v) FROM t_est WHERE b = 1 GROUP BY b SETTINGS max_rows_to_group_by = 10, group_by_overflow_mode = 'any')
WHERE match(line, '^(With |empirical_status:|empirical_reason:)');

SELECT '-- the projection part has as many groups as the limit';
SELECT replaceRegexpAll(trim(explain), '\\s+', ' ') AS line
FROM (EXPLAIN WHATIF SELECT b, sum(v) FROM t_est WHERE b = 1 GROUP BY b SETTINGS max_rows_to_group_by = 500, group_by_overflow_mode = 'throw')
WHERE match(line, '^(With |marks:|empirical_status:|verdict:)');

DROP TABLE t_est;
