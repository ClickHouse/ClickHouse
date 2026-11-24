-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- `timeSeriesSelector(..., 'tags')` returns the tags of the time series matched by the selector and by their stored time ranges.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_buckets;
DROP TABLE IF EXISTS ts_rows;
DROP TABLE IF EXISTS ts_names;
DROP TABLE IF EXISTS ts_names_rows;

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

SELECT '-- the time series whose stored time range does not intersect the range are not returned';
SELECT tags FROM timeSeriesSelector(ts_buckets, 'm', 0, 5000, 'tags') ORDER BY tags;
SELECT tags FROM timeSeriesSelector(ts_buckets, 'm', 5000, 10000, 'tags') ORDER BY tags;
SELECT count() FROM timeSeriesSelector(ts_buckets, 'm', 20000, 30000, 'tags');

SELECT '-- a time series with samples before and after the range but not in it is returned, it has no samples to join';
SELECT tags FROM timeSeriesSelector(ts_buckets, 'm', 2000, 3000, 'tags') ORDER BY tags;
SELECT count() FROM timeSeriesSelector(ts_buckets, 'm', 2000, 3000, 'samples');
SELECT count()
FROM (SELECT id FROM timeSeriesSelector(ts_buckets, 'm', 2000, 3000, 'samples') GROUP BY id) AS s
ANY INNER JOIN timeSeriesSelector(ts_buckets, 'm', 2000, 3000, 'tags') AS t ON s.id = t.id;

SELECT '-- the identifiers are the identifiers of the samples';
SELECT (SELECT groupArraySorted(10)(id) FROM timeSeriesSelector(ts_buckets, 'm', 0, 5000, 'tags'))
     = (SELECT groupArraySorted(10)(DISTINCT id) FROM timeSeriesSelector(ts_buckets, 'm', 0, 5000, 'samples'));
SELECT (SELECT groupArraySorted(10)(id) FROM timeSeriesSelector(ts_rows, 'm', 0, 5000, 'tags'))
     = (SELECT groupArraySorted(10)(DISTINCT id) FROM timeSeriesSelector(ts_rows, 'm', 0, 5000, 'timestamp_and_value'));
SELECT tags, samples
FROM (SELECT id, timeSeriesGroupArray(samples) AS samples FROM timeSeriesSelector(ts_buckets, 'm', 0, 5000, 'samples') GROUP BY id) AS s
ANY INNER JOIN timeSeriesSelector(ts_buckets, 'm', 0, 5000, 'tags') AS t ON s.id = t.id
ORDER BY tags;

SELECT '-- a NULL bound is unlimited';
SELECT tags FROM timeSeriesSelector(ts_buckets, 'm', NULL, NULL, 'tags') ORDER BY tags;
SELECT tags FROM timeSeriesSelector(ts_buckets, 'm', 5000, NULL, 'tags') ORDER BY tags;
SELECT tags FROM timeSeriesSelector(ts_rows, 'm', NULL, 1000, 'tags') ORDER BY tags;
SELECT count(), sum(length(samples)) FROM timeSeriesSelector(ts_buckets, 'm', NULL, NULL, 'samples');
SELECT count() FROM timeSeriesSelector(ts_rows, 'm', NULL, 120, 'timestamp_and_value');
SELECT count() FROM timeSeriesSelector(ts_buckets, 'm', 2000, NULL, 'samples');
DROP TABLE IF EXISTS ts_recent;
CREATE TABLE ts_recent ENGINE = TimeSeries;
INSERT INTO ts_recent (metric_name, tags, samples) SELECT 'm', map('job', 'a'), [(now64(3) - 60, 1.)];
SELECT count() FROM timeSeriesSelector(ts_recent, 'm', NULL, NULL, 'samples');
SELECT count() FROM timeSeriesSelector(ts_recent, 'm', now64(3) - 3600, NULL, 'samples');
DROP TABLE ts_recent;

SELECT '-- the tags stored in columns named like the columns of the generated queries';
CREATE TABLE ts_names ENGINE = TimeSeries SETTINGS tags_to_columns = {'job': 'series_tags', 'env': 'series_id'}, recent_samples_ttl_seconds = 0;
CREATE TABLE ts_names_rows ENGINE = TimeSeries SETTINGS version = 10, tags_to_columns = {'job': 'series_tags', 'env': 'series_id'}, recent_samples_ttl_seconds = 0;
INSERT INTO ts_names (metric_name, tags, samples) VALUES
    ('m', map('job', 'a', 'env', 'prod'), [(toDateTime64(100, 3), 1.)]),
    ('m', map('job', 'b'), [(toDateTime64(150, 3), 10.)]);
INSERT INTO ts_names_rows (metric_name, tags, samples) VALUES
    ('m', map('job', 'a', 'env', 'prod'), [(toDateTime64(100, 3), 1.)]),
    ('m', map('job', 'b'), [(toDateTime64(150, 3), 10.)]);
SELECT tags FROM timeSeriesSelector(ts_names, 'm{job="a"}', 0, 1000, 'tags');
SELECT tags FROM timeSeriesSelector(ts_names, 'm{env="prod"}', 0, 1000, 'tags');
SELECT tags, value FROM prometheusQuery(ts_names, 'm{job="a"}', 160);
SELECT tags, value FROM prometheusQuery(ts_names_rows, 'm{job="a"}', 160);

DROP TABLE ts_names_rows;
DROP TABLE ts_names;
DROP TABLE ts_rows;
DROP TABLE ts_buckets;
