-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- The tags table can store a time series in several rows (unmerged parts of a ReplacingMergeTree, or a MergeTree),
-- a PromQL query must return such a series once.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;

CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
TAGS ENGINE = MergeTree PRIMARY KEY metric_name ORDER BY (metric_name, id);

INSERT INTO ts (metric_name, tags, samples) VALUES
    ('m', map('job', 'a', 'host', 'h1'), [(toDateTime64(100, 3), 1.), (toDateTime64(130, 3), 4.), (toDateTime64(160, 3), 7.)]),
    ('m', map('job', 'a', 'host', 'h2'), [(toDateTime64(100, 3), 10.), (toDateTime64(130, 3), 40.), (toDateTime64(160, 3), 70.)]),
    ('m', map('job', 'b', 'host', 'h3'), [(toDateTime64(100, 3), 100.), (toDateTime64(130, 3), 400.), (toDateTime64(160, 3), 700.)]);

SELECT 'rows in the tags table', count() FROM timeSeriesTags(ts) SETTINGS enable_parallel_replicas = 0;

SELECT '-- instant selector';
SELECT tags, value FROM prometheusQuery(ts, 'm', 160) ORDER BY tags;
SELECT '-- range selector';
SELECT tags, samples FROM prometheusQuery(ts, 'm[1m]', 160) ORDER BY tags;
SELECT '-- rate';
SELECT tags, samples FROM prometheusQueryRange(ts, 'rate(m[1m])', 130, 160, 30) ORDER BY tags;
SELECT '-- sum by';
SELECT tags, samples FROM prometheusQueryRange(ts, 'sum by (job) (m)', 130, 160, 30) ORDER BY tags;

INSERT INTO FUNCTION timeSeriesTags(ts) SELECT * FROM timeSeriesTags(ts) SETTINGS enable_parallel_replicas = 0;

SELECT 'rows in the tags table', count() FROM timeSeriesTags(ts) SETTINGS enable_parallel_replicas = 0;

SELECT '-- instant selector';
SELECT tags, value FROM prometheusQuery(ts, 'm', 160) ORDER BY tags;
SELECT '-- range selector';
SELECT tags, samples FROM prometheusQuery(ts, 'm[1m]', 160) ORDER BY tags;
SELECT '-- rate';
SELECT tags, samples FROM prometheusQueryRange(ts, 'rate(m[1m])', 130, 160, 30) ORDER BY tags;
SELECT '-- sum by';
SELECT tags, samples FROM prometheusQueryRange(ts, 'sum by (job) (m)', 130, 160, 30) ORDER BY tags;

DROP TABLE ts;
