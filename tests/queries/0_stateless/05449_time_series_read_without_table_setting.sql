-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

SET allow_experimental_time_series_table = 1;
CREATE TABLE ts ENGINE = TimeSeries;
INSERT INTO ts (metric_name, tags, samples) VALUES ('m', {'job': 'j'}, [(toDateTime64(100, 3), 1.5), (toDateTime64(200, 3), 2.5)]);

-- The setting gates creating a TimeSeries table. Reading an existing table doesn't need it, including the functions
-- which the generated queries use for a table storing buckets of samples.
SET allow_experimental_time_series_table = 0;
SELECT toUnixTimestamp64Milli(timestamp), value FROM timeSeriesSelector(ts, 'm', 0, 300, 'timestamp_and_value') ORDER BY timestamp;
SELECT arrayMap(s -> (toUnixTimestamp64Milli(s.1), s.2), samples) FROM timeSeriesSelector(ts, 'm', 150, 300, 'samples');
SELECT metric_name, arrayMap(s -> (toUnixTimestamp64Milli(s.1), s.2), samples) FROM ts;
DROP TABLE ts;
