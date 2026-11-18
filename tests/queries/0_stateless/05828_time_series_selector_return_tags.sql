-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- `timeSeriesSelector(..., 'tags')` returns the tags of the time series whose samples the other values of `return_columns` read.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_buckets;
DROP TABLE IF EXISTS ts_rows;

CREATE TABLE ts_buckets ENGINE = TimeSeries SETTINGS tags_to_columns = {'job': 'job_column'}, recent_samples_ttl_seconds = 0;
CREATE TABLE ts_rows ENGINE = TimeSeries SETTINGS version = 10, tags_to_columns = {'job': 'job_column'}, recent_samples_ttl_seconds = 0;

INSERT INTO ts_buckets (metric_name, tags, samples) VALUES
    ('m', map('job', 'a', 'env', 'prod'), [(toDateTime64(100, 3), 1.), (toDateTime64(4000, 3), 2.)]),
    ('m', map('job', 'b'), [(toDateTime64(150, 3), 10.)]),
    ('m', map('env', 'dev'), [(toDateTime64(8000, 3), 20.)]),
    ('other', map('job', 'a'), [(toDateTime64(100, 3), 30.)]);
INSERT INTO ts_rows (metric_name, tags, samples) VALUES
    ('m', map('job', 'a', 'env', 'prod'), [(toDateTime64(100, 3), 1.), (toDateTime64(4000, 3), 2.)]),
    ('m', map('job', 'b'), [(toDateTime64(150, 3), 10.)]),
    ('m', map('env', 'dev'), [(toDateTime64(8000, 3), 20.)]),
    ('other', map('job', 'a'), [(toDateTime64(100, 3), 30.)]);

SELECT '-- the returned columns';
DESCRIBE timeSeriesSelector(ts_buckets, 'm', 0, 5000, 'tags');
DESCRIBE timeSeriesSelector(ts_rows, 'm', 0, 5000, 'tags');

SELECT '-- the tags include `__name__` and the tags stored in separate columns, sorted by name';
SELECT tags FROM timeSeriesSelector(ts_buckets, 'm', 0, 10000, 'tags') ORDER BY tags;
SELECT tags FROM timeSeriesSelector(ts_rows, 'm', 0, 10000, 'tags') ORDER BY tags;
SELECT tags FROM timeSeriesSelector(ts_buckets, '{job="a"}', 0, 10000, 'tags') ORDER BY tags;

SELECT '-- the time series without samples in the time range are not returned';
SELECT tags FROM timeSeriesSelector(ts_buckets, 'm', 0, 5000, 'tags') ORDER BY tags;
SELECT tags FROM timeSeriesSelector(ts_buckets, 'm', 5000, 10000, 'tags') ORDER BY tags;
SELECT count() FROM timeSeriesSelector(ts_buckets, 'm', 20000, 30000, 'tags');

SELECT '-- the identifiers are the identifiers of the samples';
SELECT (SELECT groupArraySorted(10)(id) FROM timeSeriesSelector(ts_buckets, 'm', 0, 5000, 'tags'))
     = (SELECT groupArraySorted(10)(DISTINCT id) FROM timeSeriesSelector(ts_buckets, 'm', 0, 5000, 'samples'));
SELECT (SELECT groupArraySorted(10)(id) FROM timeSeriesSelector(ts_rows, 'm', 0, 5000, 'tags'))
     = (SELECT groupArraySorted(10)(DISTINCT id) FROM timeSeriesSelector(ts_rows, 'm', 0, 5000, 'timestamp_and_value'));
SELECT tags, samples
FROM (SELECT id, timeSeriesGroupArray(samples) AS samples FROM timeSeriesSelector(ts_buckets, 'm', 0, 5000, 'samples') GROUP BY id) AS s
ANY INNER JOIN timeSeriesSelector(ts_buckets, 'm', 0, 5000, 'tags') AS t ON s.id = t.id
ORDER BY tags;

DROP TABLE ts_rows;
DROP TABLE ts_buckets;
