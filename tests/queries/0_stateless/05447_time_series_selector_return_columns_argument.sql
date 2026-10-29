-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- The argument `return_columns` of `timeSeriesSelector` chooses the columns returning the samples: 'samples' (an array of samples per row) or
-- 'timestamp_and_value' (one sample per row). The samples are converted when the samples table stores them the other way:
-- a table of version 11 or later stores buckets of samples as arrays, the tables of earlier versions store one sample per row.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_buckets;
DROP TABLE IF EXISTS ts_rows;

CREATE TABLE ts_buckets ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0;
CREATE TABLE ts_rows ENGINE = TimeSeries SETTINGS version = 10, recent_samples_ttl_seconds = 0;

-- The samples of `prod` fall into two one-hour buckets.
INSERT INTO ts_buckets (metric_name, tags, samples) VALUES
    ('m', map('env', 'prod'), [(toDateTime64(100, 3), 1.), (toDateTime64(4000, 3), 2.)]),
    ('m', map('env', 'dev'), [(toDateTime64(150, 3), 10.)]);
INSERT INTO ts_rows (metric_name, tags, samples) VALUES
    ('m', map('env', 'prod'), [(toDateTime64(100, 3), 1.), (toDateTime64(4000, 3), 2.)]),
    ('m', map('env', 'dev'), [(toDateTime64(150, 3), 10.)]);

SELECT '-- the returned columns for each layout of the table';
DESCRIBE timeSeriesSelector(ts_buckets, 'm', 0, 5000, 'samples');
DESCRIBE timeSeriesSelector(ts_buckets, 'm', 0, 5000, 'timestamp_and_value');
DESCRIBE timeSeriesSelector(ts_rows, 'm', 0, 5000, 'samples');
DESCRIBE timeSeriesSelector(ts_rows, 'm', 0, 5000, 'timestamp_and_value');

SELECT '-- the same samples are returned whatever the layout and the requested columns are';
SELECT 'buckets as samples', timeSeriesGroupArray(samples) FROM timeSeriesSelector(ts_buckets, 'm{env="prod"}', 0, 5000, 'samples') GROUP BY id;
SELECT 'buckets as rows', timestamp, value FROM timeSeriesSelector(ts_buckets, 'm{env="prod"}', 0, 5000, 'timestamp_and_value') ORDER BY timestamp;
SELECT 'rows as samples', timeSeriesGroupArray(samples) FROM timeSeriesSelector(ts_rows, 'm{env="prod"}', 0, 5000, 'samples') GROUP BY id;
SELECT 'rows as rows', timestamp, value FROM timeSeriesSelector(ts_rows, 'm{env="prod"}', 0, 5000, 'timestamp_and_value') ORDER BY timestamp;

SELECT '-- the time range is applied before the conversion';
SELECT count(), min(timestamp), max(timestamp) FROM timeSeriesSelector(ts_buckets, 'm', 120, 4000, 'timestamp_and_value');
SELECT sum(length(samples)) FROM timeSeriesSelector(ts_rows, 'm', 120, 4000, 'samples');

SELECT '-- the argument is required and must be one of the two values';
SELECT count() FROM timeSeriesSelector(ts_buckets, 'm', 0, 5000); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT count() FROM timeSeriesSelector(ts_buckets, 'm', 0, 5000, 'rows'); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM timeSeriesSelector(ts_buckets, 'm', 0, 5000, CAST(NULL, 'Nullable(String)')); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM timeSeriesSelector(ts_buckets, 'm', 0, 5000, toLowCardinality('samples'));

DROP TABLE ts_rows;
DROP TABLE ts_buckets;
