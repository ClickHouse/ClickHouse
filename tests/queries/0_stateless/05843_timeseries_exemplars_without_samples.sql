-- A time series which has exemplars and no samples is read with empty `samples`, also when `samples` is selected.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
DROP TABLE IF EXISTS ts_dst;
CREATE TABLE ts ENGINE = TimeSeries SETTINGS store_exemplars = 1;
CREATE TABLE ts_dst ENGINE = TimeSeries SETTINGS store_exemplars = 1;

INSERT INTO ts (metric_name, exemplars) SELECT 'only_exemplars', [(toDateTime64(100, 3), 1., map('trace_id', 't'))];
INSERT INTO ts (metric_name, samples) SELECT 'only_samples', [(toDateTime64(100, 3), 1.)];
-- The samples of this series are in two buckets.
INSERT INTO ts (metric_name, samples, exemplars) SELECT 'both', [(toDateTime64(100, 3), 1.), (toDateTime64(100000, 3), 2.)], [(toDateTime64(100, 3), 2., map('trace_id', 'b'))];
INSERT INTO ts (metric_name) SELECT 'neither';

SELECT '-- exemplars';
SELECT metric_name, exemplars FROM ts FINAL ORDER BY ALL;
SELECT '-- samples';
SELECT metric_name, samples FROM ts FINAL ORDER BY ALL;
SELECT '-- samples and exemplars';
SELECT metric_name, samples, exemplars FROM ts FINAL ORDER BY ALL;
SELECT '-- without FINAL';
SELECT metric_name, samples, exemplars FROM ts ORDER BY ALL;
SELECT '-- all columns';
SELECT * FROM ts FINAL ORDER BY ALL;
SELECT '-- with a filter';
SELECT metric_name, samples, exemplars FROM ts FINAL WHERE metric_name = 'only_exemplars';
SELECT '-- with other join algorithms';
SELECT metric_name, samples, exemplars FROM ts FINAL ORDER BY ALL SETTINGS join_algorithm = 'full_sorting_merge';
SELECT metric_name, samples, exemplars FROM ts FINAL ORDER BY ALL SETTINGS join_algorithm = 'grace_hash';

SELECT '-- a copy keeps the exemplars';
INSERT INTO ts_dst SELECT * FROM ts FINAL;
SELECT metric_name, samples, exemplars FROM ts_dst FINAL ORDER BY ALL;

DROP TABLE ts;
DROP TABLE ts_dst;
