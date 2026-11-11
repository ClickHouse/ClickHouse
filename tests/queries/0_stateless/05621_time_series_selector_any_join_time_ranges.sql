-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
--
-- The selector filters time series by time with a join of the tags table and the time ranges table.
-- The time ranges table keeps one row per time series per insert until its parts are merged, so the join is `ANY`:
-- a tags row is not repeated for every unmerged row of its time series. The filtering itself is checked
-- in 05255_timeseries_time_ranges_filter.sql.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

-- Two inserts write two rows of the time ranges table for the same time series.
INSERT INTO ts (metric_name, tags, samples) VALUES ('m', map('host', 'h1'), [(toDateTime64(1000, 3), 1.)]);
INSERT INTO ts (metric_name, tags, samples) VALUES ('m', map('host', 'h1'), [(toDateTime64(2000, 3), 2.)]);

SELECT count() AS series, sum(length(samples)) AS samples FROM timeSeriesSelector(ts, 'm', 0, 2500, 'samples');

SELECT countIf(explain LIKE '%Strictness: ANY%') AS any_joins,
       countIf(explain LIKE '%Strictness: ALL%') AS all_joins,
       countIf(explain LIKE '%Type: LEFT%') AS left_joins,
       countIf(explain LIKE '%Algorithm: FullSortingMergeJoin%') AS merge_joins
FROM (EXPLAIN PLAN actions = 1 SELECT count() FROM timeSeriesSelector(ts, 'm', 0, 2500, 'samples'));

DROP TABLE ts;
