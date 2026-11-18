-- Tags: no-fasttest, no-parallel-replicas
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: every parallel replica reads all rows of the tables behind `timeSeriesTimeRanges`, so each row
-- is returned once per replica until https://github.com/ClickHouse/ClickHouse/pull/119350 is merged.

-- The argument `return_columns` of `timeSeriesSelector`:
--   'series' returns `id` and `tags` (a sorted array of pairs, made by `timeSeriesNormalizeTags`) of the time series, one row per time series,
--     without reading the samples; the tags are the same as `timeSeriesIdToTags(id)` gives for the same time series read with 'samples';
--   'samples_without_tags' returns the same as 'samples' but doesn't register the tags for `timeSeriesIdToTags`.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_cols;
DROP TABLE IF EXISTS ts_plain;
DROP TABLE IF EXISTS ts_old;
DROP TABLE IF EXISTS ts_ranges;

CREATE TABLE ts_cols ENGINE = TimeSeries SETTINGS tags_to_columns = {'job': 'job_column', 'instance': 'instance_column'}, recent_samples_ttl_seconds = 0;
CREATE TABLE ts_plain ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0;
CREATE TABLE ts_old ENGINE = TimeSeries SETTINGS version = 10, recent_samples_ttl_seconds = 0;

INSERT INTO ts_cols (metric_name, tags, samples) VALUES
    ('http_requests', {'job': 'crawler', 'instance': 'host1:8080', 'region': 'eu'}, [(toDateTime64(100, 3), 1.), (toDateTime64(200, 3), 2.)]),
    ('http_requests', {'job': 'crawler', 'instance': 'host2:8080'}, [(toDateTime64(100, 3), 3.), (toDateTime64(4000, 3), 4.)]),
    ('http_requests', {'region': 'us'}, [(toDateTime64(150, 3), 5.)]),
    ('http_errors', {'job': 'api', 'instance': 'host1:8080', 'code': '500'}, [(toDateTime64(4000, 3), 6.)]),
    ('up', {}, [(toDateTime64(100, 3), 1.)]);

INSERT INTO ts_plain (metric_name, tags, samples) VALUES
    ('m', {'b': '2', 'a': '1', 'z': '', 'c': '3'}, [(toDateTime64(100, 3), 1.)]),
    ('m', {'a': '1'}, [(toDateTime64(100, 3), 2.)]),
    ('n', {}, [(toDateTime64(100, 3), 3.)]);

INSERT INTO ts_old (metric_name, tags, samples) VALUES
    ('m', {'a': '1', 'b': '2'}, [(toDateTime64(100, 3), 1.), (toDateTime64(4000, 3), 2.)]),
    ('m', {'a': '3'}, [(toDateTime64(100, 3), 3.)]);

SELECT '-- the returned columns';
DESCRIBE timeSeriesSelector(ts_cols, 'http_requests', 0, 5000, 'series');
DESCRIBE timeSeriesSelector(ts_cols, 'http_requests', 0, 5000, 'samples_without_tags');

SELECT '-- the series with their tags: metric name and tag columns';
SELECT tags FROM timeSeriesSelector(ts_cols, '{__name__=~"http_.*"}', 0, 5000, 'series') ORDER BY tags;

SELECT '-- the tags are sorted, a tag with an empty value is dropped, no tag columns';
SELECT tags FROM timeSeriesSelector(ts_plain, '{__name__=~".+"}', 0, 5000, 'series') ORDER BY tags;

SELECT '-- the id has the type of the id of the tags table, the same as with the samples';
SELECT toTypeName(id) = (SELECT toTypeName(id) FROM timeSeriesSelector(ts_cols, 'up', 0, 5000, 'samples') LIMIT 1)
FROM timeSeriesSelector(ts_cols, 'up', 0, 5000, 'series');

-- The tags of 'series' must equal timeSeriesIdToTags(id) of the same time series read with 'samples'.
-- Each query below prints the number of time series returned by 'series' and by 'samples', how many of them are returned by both
-- but have different tags, and how many are returned by 'samples' but not by 'series'.
SELECT '-- series vs timeSeriesIdToTags: metric name matchers and tag columns';
SELECT 'ts_cols http_requests [0, 5000]' AS test, countIf(s_tags != []) AS num_series, countIf(p_tags != []) AS num_samples,
    countIf(s_tags != [] AND p_tags != [] AND s_tags != p_tags) AS different, countIf(s_tags = [] AND p_tags != []) AS missing_in_series
FROM (SELECT id, tags AS s_tags FROM timeSeriesSelector(ts_cols, 'http_requests', 0, 5000, 'series')) AS s
FULL JOIN (SELECT id, arraySort(timeSeriesIdToTags(id)) AS p_tags FROM timeSeriesSelector(ts_cols, 'http_requests', 0, 5000, 'samples') GROUP BY id) AS p USING (id);
SELECT 'ts_cols {job="crawler"} [0, 5000]' AS test, countIf(s_tags != []) AS num_series, countIf(p_tags != []) AS num_samples,
    countIf(s_tags != [] AND p_tags != [] AND s_tags != p_tags) AS different, countIf(s_tags = [] AND p_tags != []) AS missing_in_series
FROM (SELECT id, tags AS s_tags FROM timeSeriesSelector(ts_cols, '{job="crawler"}', 0, 5000, 'series')) AS s
FULL JOIN (SELECT id, arraySort(timeSeriesIdToTags(id)) AS p_tags FROM timeSeriesSelector(ts_cols, '{job="crawler"}', 0, 5000, 'samples') GROUP BY id) AS p USING (id);
SELECT 'ts_cols http_requests{region=~"e.|us"} [0, 5000]' AS test, countIf(s_tags != []) AS num_series, countIf(p_tags != []) AS num_samples,
    countIf(s_tags != [] AND p_tags != [] AND s_tags != p_tags) AS different, countIf(s_tags = [] AND p_tags != []) AS missing_in_series
FROM (SELECT id, tags AS s_tags FROM timeSeriesSelector(ts_cols, 'http_requests{region=~"e.|us"}', 0, 5000, 'series')) AS s
FULL JOIN (SELECT id, arraySort(timeSeriesIdToTags(id)) AS p_tags FROM timeSeriesSelector(ts_cols, 'http_requests{region=~"e.|us"}', 0, 5000, 'samples') GROUP BY id) AS p USING (id);
SELECT 'ts_cols {__name__=~".+", __name__!="up", job!="api"} [0, 5000]' AS test, countIf(s_tags != []) AS num_series, countIf(p_tags != []) AS num_samples,
    countIf(s_tags != [] AND p_tags != [] AND s_tags != p_tags) AS different, countIf(s_tags = [] AND p_tags != []) AS missing_in_series
FROM (SELECT id, tags AS s_tags FROM timeSeriesSelector(ts_cols, '{__name__=~".+", __name__!="up", job!="api"}', 0, 5000, 'series')) AS s
FULL JOIN (SELECT id, arraySort(timeSeriesIdToTags(id)) AS p_tags FROM timeSeriesSelector(ts_cols, '{__name__=~".+", __name__!="up", job!="api"}', 0, 5000, 'samples') GROUP BY id) AS p USING (id);
SELECT 'ts_cols {instance="host1:8080"} [0, 5000]' AS test, countIf(s_tags != []) AS num_series, countIf(p_tags != []) AS num_samples,
    countIf(s_tags != [] AND p_tags != [] AND s_tags != p_tags) AS different, countIf(s_tags = [] AND p_tags != []) AS missing_in_series
FROM (SELECT id, tags AS s_tags FROM timeSeriesSelector(ts_cols, '{instance="host1:8080"}', 0, 5000, 'series')) AS s
FULL JOIN (SELECT id, arraySort(timeSeriesIdToTags(id)) AS p_tags FROM timeSeriesSelector(ts_cols, '{instance="host1:8080"}', 0, 5000, 'samples') GROUP BY id) AS p USING (id);
SELECT 'ts_cols {__name__=~".+", job!="crawler"} [0, 5000]' AS test, countIf(s_tags != []) AS num_series, countIf(p_tags != []) AS num_samples,
    countIf(s_tags != [] AND p_tags != [] AND s_tags != p_tags) AS different, countIf(s_tags = [] AND p_tags != []) AS missing_in_series
FROM (SELECT id, tags AS s_tags FROM timeSeriesSelector(ts_cols, '{__name__=~".+", job!="crawler"}', 0, 5000, 'series')) AS s
FULL JOIN (SELECT id, arraySort(timeSeriesIdToTags(id)) AS p_tags FROM timeSeriesSelector(ts_cols, '{__name__=~".+", job!="crawler"}', 0, 5000, 'samples') GROUP BY id) AS p USING (id);
SELECT 'ts_cols {__name__=~"http_.*", job=~"cr.*|api"} [0, 5000]' AS test, countIf(s_tags != []) AS num_series, countIf(p_tags != []) AS num_samples,
    countIf(s_tags != [] AND p_tags != [] AND s_tags != p_tags) AS different, countIf(s_tags = [] AND p_tags != []) AS missing_in_series
FROM (SELECT id, tags AS s_tags FROM timeSeriesSelector(ts_cols, '{__name__=~"http_.*", job=~"cr.*|api"}', 0, 5000, 'series')) AS s
FULL JOIN (SELECT id, arraySort(timeSeriesIdToTags(id)) AS p_tags FROM timeSeriesSelector(ts_cols, '{__name__=~"http_.*", job=~"cr.*|api"}', 0, 5000, 'samples') GROUP BY id) AS p USING (id);
SELECT 'ts_cols nothing [0, 5000]' AS test, countIf(s_tags != []) AS num_series, countIf(p_tags != []) AS num_samples,
    countIf(s_tags != [] AND p_tags != [] AND s_tags != p_tags) AS different, countIf(s_tags = [] AND p_tags != []) AS missing_in_series
FROM (SELECT id, tags AS s_tags FROM timeSeriesSelector(ts_cols, 'nothing', 0, 5000, 'series')) AS s
FULL JOIN (SELECT id, arraySort(timeSeriesIdToTags(id)) AS p_tags FROM timeSeriesSelector(ts_cols, 'nothing', 0, 5000, 'samples') GROUP BY id) AS p USING (id);

SELECT '-- series vs timeSeriesIdToTags: no tag columns';
SELECT 'ts_plain m [0, 5000]' AS test, countIf(s_tags != []) AS num_series, countIf(p_tags != []) AS num_samples,
    countIf(s_tags != [] AND p_tags != [] AND s_tags != p_tags) AS different, countIf(s_tags = [] AND p_tags != []) AS missing_in_series
FROM (SELECT id, tags AS s_tags FROM timeSeriesSelector(ts_plain, 'm', 0, 5000, 'series')) AS s
FULL JOIN (SELECT id, arraySort(timeSeriesIdToTags(id)) AS p_tags FROM timeSeriesSelector(ts_plain, 'm', 0, 5000, 'samples') GROUP BY id) AS p USING (id);
SELECT 'ts_plain {__name__=~".+"} [0, 5000]' AS test, countIf(s_tags != []) AS num_series, countIf(p_tags != []) AS num_samples,
    countIf(s_tags != [] AND p_tags != [] AND s_tags != p_tags) AS different, countIf(s_tags = [] AND p_tags != []) AS missing_in_series
FROM (SELECT id, tags AS s_tags FROM timeSeriesSelector(ts_plain, '{__name__=~".+"}', 0, 5000, 'series')) AS s
FULL JOIN (SELECT id, arraySort(timeSeriesIdToTags(id)) AS p_tags FROM timeSeriesSelector(ts_plain, '{__name__=~".+"}', 0, 5000, 'samples') GROUP BY id) AS p USING (id);
SELECT 'ts_plain m{a="1", c="3"} [0, 5000]' AS test, countIf(s_tags != []) AS num_series, countIf(p_tags != []) AS num_samples,
    countIf(s_tags != [] AND p_tags != [] AND s_tags != p_tags) AS different, countIf(s_tags = [] AND p_tags != []) AS missing_in_series
FROM (SELECT id, tags AS s_tags FROM timeSeriesSelector(ts_plain, 'm{a="1", c="3"}', 0, 5000, 'series')) AS s
FULL JOIN (SELECT id, arraySort(timeSeriesIdToTags(id)) AS p_tags FROM timeSeriesSelector(ts_plain, 'm{a="1", c="3"}', 0, 5000, 'samples') GROUP BY id) AS p USING (id);

-- The time range: 'series' filters the time series by the time range of all their samples (the min and max time), not by their samples,
-- so it can return a time series which has no samples in the range, while 'samples' returns only the time series with samples in the range.
SELECT '-- series vs timeSeriesIdToTags: time range';
SELECT 'ts_cols http_requests [0, 120]' AS test, countIf(s_tags != []) AS num_series, countIf(p_tags != []) AS num_samples,
    countIf(s_tags != [] AND p_tags != [] AND s_tags != p_tags) AS different, countIf(s_tags = [] AND p_tags != []) AS missing_in_series
FROM (SELECT id, tags AS s_tags FROM timeSeriesSelector(ts_cols, 'http_requests', 0, 120, 'series')) AS s
FULL JOIN (SELECT id, arraySort(timeSeriesIdToTags(id)) AS p_tags FROM timeSeriesSelector(ts_cols, 'http_requests', 0, 120, 'samples') GROUP BY id) AS p USING (id);
SELECT 'ts_cols http_requests [3000, 5000]' AS test, countIf(s_tags != []) AS num_series, countIf(p_tags != []) AS num_samples,
    countIf(s_tags != [] AND p_tags != [] AND s_tags != p_tags) AS different, countIf(s_tags = [] AND p_tags != []) AS missing_in_series
FROM (SELECT id, tags AS s_tags FROM timeSeriesSelector(ts_cols, 'http_requests', 3000, 5000, 'series')) AS s
FULL JOIN (SELECT id, arraySort(timeSeriesIdToTags(id)) AS p_tags FROM timeSeriesSelector(ts_cols, 'http_requests', 3000, 5000, 'samples') GROUP BY id) AS p USING (id);
SELECT 'ts_cols {__name__=~".+"} [3900, 4100]' AS test, countIf(s_tags != []) AS num_series, countIf(p_tags != []) AS num_samples,
    countIf(s_tags != [] AND p_tags != [] AND s_tags != p_tags) AS different, countIf(s_tags = [] AND p_tags != []) AS missing_in_series
FROM (SELECT id, tags AS s_tags FROM timeSeriesSelector(ts_cols, '{__name__=~".+"}', 3900, 4100, 'series')) AS s
FULL JOIN (SELECT id, arraySort(timeSeriesIdToTags(id)) AS p_tags FROM timeSeriesSelector(ts_cols, '{__name__=~".+"}', 3900, 4100, 'samples') GROUP BY id) AS p USING (id);
SELECT 'ts_cols http_requests [1000, 3000]' AS test, countIf(s_tags != []) AS num_series, countIf(p_tags != []) AS num_samples,
    countIf(s_tags != [] AND p_tags != [] AND s_tags != p_tags) AS different, countIf(s_tags = [] AND p_tags != []) AS missing_in_series
FROM (SELECT id, tags AS s_tags FROM timeSeriesSelector(ts_cols, 'http_requests', 1000, 3000, 'series')) AS s
FULL JOIN (SELECT id, arraySort(timeSeriesIdToTags(id)) AS p_tags FROM timeSeriesSelector(ts_cols, 'http_requests', 1000, 3000, 'samples') GROUP BY id) AS p USING (id);
SELECT 'ts_cols http_requests [9000, 9001]' AS test, countIf(s_tags != []) AS num_series, countIf(p_tags != []) AS num_samples,
    countIf(s_tags != [] AND p_tags != [] AND s_tags != p_tags) AS different, countIf(s_tags = [] AND p_tags != []) AS missing_in_series
FROM (SELECT id, tags AS s_tags FROM timeSeriesSelector(ts_cols, 'http_requests', 9000, 9001, 'series')) AS s
FULL JOIN (SELECT id, arraySort(timeSeriesIdToTags(id)) AS p_tags FROM timeSeriesSelector(ts_cols, 'http_requests', 9000, 9001, 'samples') GROUP BY id) AS p USING (id);
SELECT 'ts_old m [0, 5000]' AS test, countIf(s_tags != []) AS num_series, countIf(p_tags != []) AS num_samples,
    countIf(s_tags != [] AND p_tags != [] AND s_tags != p_tags) AS different, countIf(s_tags = [] AND p_tags != []) AS missing_in_series
FROM (SELECT id, tags AS s_tags FROM timeSeriesSelector(ts_old, 'm', 0, 5000, 'series')) AS s
FULL JOIN (SELECT id, arraySort(timeSeriesIdToTags(id)) AS p_tags FROM timeSeriesSelector(ts_old, 'm', 0, 5000, 'samples') GROUP BY id) AS p USING (id);
SELECT 'ts_old m [3000, 5000]' AS test, countIf(s_tags != []) AS num_series, countIf(p_tags != []) AS num_samples,
    countIf(s_tags != [] AND p_tags != [] AND s_tags != p_tags) AS different, countIf(s_tags = [] AND p_tags != []) AS missing_in_series
FROM (SELECT id, tags AS s_tags FROM timeSeriesSelector(ts_old, 'm', 3000, 5000, 'series')) AS s
FULL JOIN (SELECT id, arraySort(timeSeriesIdToTags(id)) AS p_tags FROM timeSeriesSelector(ts_old, 'm', 3000, 5000, 'samples') GROUP BY id) AS p USING (id);
SELECT 'ts_old m [1000, 3000]' AS test, countIf(s_tags != []) AS num_series, countIf(p_tags != []) AS num_samples,
    countIf(s_tags != [] AND p_tags != [] AND s_tags != p_tags) AS different, countIf(s_tags = [] AND p_tags != []) AS missing_in_series
FROM (SELECT id, tags AS s_tags FROM timeSeriesSelector(ts_old, 'm', 1000, 3000, 'series')) AS s
FULL JOIN (SELECT id, arraySort(timeSeriesIdToTags(id)) AS p_tags FROM timeSeriesSelector(ts_old, 'm', 1000, 3000, 'samples') GROUP BY id) AS p USING (id);

SELECT '-- time range: the tags returned';
SELECT tags FROM timeSeriesSelector(ts_cols, 'http_requests', 3000, 5000, 'series') ORDER BY tags;
SELECT tags FROM timeSeriesSelector(ts_old, 'm', 1000, 3000, 'series') ORDER BY tags;

-- A time series hidden by the time ranges table: it is written directly into the inner tables with a time range far from its samples.
-- The tables are qualified because an asynchronous insert through a table function resolves an unqualified name in the default database.
SELECT '-- time ranges table';
CREATE TABLE ts_ranges ENGINE = TimeSeries TAGS INNER COLUMNS (id UInt64);
INSERT INTO ts_ranges (metric_name, tags, samples) VALUES ('m', map('host', 'h2'), [(toDateTime64(2000, 3), 3.)]);
INSERT INTO FUNCTION timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts_ranges) (id, metric_name, tags) VALUES (1, 'm', map('__name__', 'm', 'host', 'h1'));
INSERT INTO FUNCTION timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts_ranges) (id, samples, bucket, min_time, max_time) VALUES
    (1, [(toDateTime64(1000, 3), 1.), (toDateTime64(1060, 3), 2.)], toDateTime64(0, 3), toDateTime64(1000, 3), toDateTime64(1060, 3));
INSERT INTO FUNCTION timeSeriesTimeRanges({CLICKHOUSE_DATABASE:Identifier}.ts_ranges) (id, min_time, max_time) VALUES (1, toDateTime64(3000, 3), toDateTime64(3000, 3));
SELECT tags FROM timeSeriesSelector(ts_ranges, 'm', 0, 2500, 'series') ORDER BY tags;
SELECT tags FROM timeSeriesSelector(ts_ranges, 'm', 2900, 3100, 'series') ORDER BY tags;
SELECT tags FROM timeSeriesSelector(ts_ranges, '{host="h1"}', 0, 2500, 'series') ORDER BY tags;

SELECT '-- samples_without_tags returns the same as samples';
SELECT (SELECT arraySort(groupArray((id, samples))) FROM timeSeriesSelector(ts_cols, 'http_requests', 0, 5000, 'samples'))
     = (SELECT arraySort(groupArray((id, samples))) FROM timeSeriesSelector(ts_cols, 'http_requests', 0, 5000, 'samples_without_tags'));
SELECT (SELECT arraySort(groupArray((id, samples))) FROM timeSeriesSelector(ts_old, 'm', 0, 5000, 'samples'))
     = (SELECT arraySort(groupArray((id, samples))) FROM timeSeriesSelector(ts_old, 'm', 0, 5000, 'samples_without_tags'));

SELECT '-- samples_without_tags does not register the tags';
SELECT timeSeriesIdToTags(id) FROM timeSeriesSelector(ts_cols, 'up', 0, 5000, 'samples_without_tags'); -- { serverError BAD_ARGUMENTS }

SELECT '-- join series with samples_without_tags by id';
SELECT s.tags, arraySum(arrayMap(x -> x.2, p.samples)) AS sum_values
FROM timeSeriesSelector(ts_cols, 'http_requests', 0, 5000, 'series') AS s
INNER JOIN (SELECT id, timeSeriesGroupArray(samples) AS samples FROM timeSeriesSelector(ts_cols, 'http_requests', 0, 5000, 'samples_without_tags') GROUP BY id) AS p USING (id)
ORDER BY s.tags;

SELECT '-- wrong return_columns';
SELECT * FROM timeSeriesSelector(ts_cols, 'up', 0, 5000, 'tags'); -- { serverError BAD_ARGUMENTS }

DROP TABLE ts_cols;
DROP TABLE ts_plain;
DROP TABLE ts_old;
DROP TABLE ts_ranges;
