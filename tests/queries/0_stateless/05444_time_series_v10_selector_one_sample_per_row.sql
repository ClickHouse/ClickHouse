-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- A TimeSeries table of version 10 or earlier stores one sample per row, so `timeSeriesSelector` returns the columns
-- `timestamp` and `value` instead of the column `samples` of a table storing buckets of samples, and PromQL reads them as they are.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_v10;
CREATE TABLE ts_v10 ENGINE = TimeSeries SETTINGS version = 10, recent_samples_ttl_seconds = 0;

-- The last sample of the series `prod` is a stale marker (a NaN with a special payload).
INSERT INTO ts_v10 (metric_name, tags, samples) VALUES
    ('m', map('env', 'prod'), [(toDateTime64(100, 3), 1.), (toDateTime64(160, 3), 2.), (toDateTime64(220, 3), 3.), (toDateTime64(280, 3), reinterpret(0x7ff0000000000002, 'Float64'))]),
    ('m', map('env', 'dev'), [(toDateTime64(130, 3), 10.), (toDateTime64(190, 3), 20.)]);

SELECT '-- timeSeriesSelector returns one sample per row';
DESCRIBE timeSeriesSelector(ts_v10, 'm', 0, 1, 'timestamp_and_value');
SELECT timestamp, value FROM timeSeriesSelector(ts_v10, 'm{env="prod"}', 150, 250, 'timestamp_and_value') ORDER BY timestamp;
SELECT count() FROM timeSeriesSelector(ts_v10, 'm', 0, 1000, 'timestamp_and_value');

SELECT '-- a range selector skips the stale marker and merges the rows of a series into one array';
SELECT * FROM prometheusQuery(ts_v10, 'm[5m]', 300) ORDER BY tags;

SELECT '-- an instant selector returns the last sample, a stale marker hides the series';
SELECT * FROM prometheusQuery(ts_v10, 'm', 200) ORDER BY tags;
SELECT * FROM prometheusQuery(ts_v10, 'm', 300) ORDER BY tags;

SELECT '-- offset and timestamp()';
SELECT * FROM prometheusQuery(ts_v10, 'm offset 1m', 260) ORDER BY tags;
SELECT * FROM prometheusQuery(ts_v10, 'timestamp(m)', 200) ORDER BY tags;

SELECT '-- a function over a range';
SELECT * FROM prometheusQueryRange(ts_v10, 'count_over_time(m[2m])', 160, 280, 60) ORDER BY tags;

DROP TABLE ts_v10;
