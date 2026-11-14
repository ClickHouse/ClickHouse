-- Tags: no-fasttest
-- Tag no-fasttest: `timeSeriesSelector` parses the selector with the PromQL parser, which needs ANTLR4, disabled in the fast-test build.

-- The recent samples table with a bucket step other than the samples table's: the histograms of an INSERT are split into the
-- buckets of each table, the counter reset hints are detected once over the whole series and shared by the rows of both tables
-- (see TimeSeriesSink), and `timeSeriesSelector` returns the same samples from either table, one row per bucket of the table read.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;

-- Hourly buckets in the samples table, one-minute buckets in the recent samples table. Its TTL is 50 years, so the samples of
-- 2024 stay in it and the test doesn't depend on now().
CREATE TABLE ts ENGINE = TimeSeries SETTINGS samples_bucket_step_seconds = 3600, recent_samples_bucket_step_seconds = 60, recent_samples_ttl_seconds = 1576800000;

-- Unsorted; sorted, the counts go 1, 3, 2: the third sample is a counter reset.
INSERT INTO ts (metric_name, tags, samples, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_int)
    VALUES ('m', map('job', 'api'), [(toDateTime64('2024-08-20 00:00:05', 3), 0.5)],
            [toDateTime64('2024-08-20 00:02:10', 3), toDateTime64('2024-08-20 00:00:10', 3), toDateTime64('2024-08-20 00:01:10', 3)],
            [2, 1, 3], [2, 1, 3], [[(0, 2)], [(0, 1)], [(0, 3)]]);

SELECT '-- the samples table: one row for the hour, the hints detected over the whole series';
SELECT bucket, min_time, max_time, samples, arrayMap(h -> (h.timestamp, h.count_int, h.counter_reset_hint), histograms)
FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts) ORDER BY bucket;

SELECT '-- the recent samples table: one row per minute with the same hints, the float sample in the first one';
SELECT bucket, min_time, max_time, samples, arrayMap(h -> (h.timestamp, h.count_int, h.counter_reset_hint), histograms)
FROM merge(currentDatabase(), '^\\.inner_id\\.recentsamples\\.') ORDER BY bucket;

SELECT '-- the selector returns the same samples from either table: a row per bucket of the table read';
SET time_series_prefer_recent_samples_table = 0;
SELECT count(), arraySort(groupArrayArray(arrayMap(h -> (h.timestamp, h.count_int, h.counter_reset_hint), histograms))), groupArrayArray(samples)
FROM timeSeriesSelector({CLICKHOUSE_DATABASE:Identifier}.ts, 'm', toDateTime64('2024-08-20 00:00:00', 3), toDateTime64('2024-08-20 00:05:00', 3), 'samples');
SET time_series_prefer_recent_samples_table = 1;
SELECT count(), arraySort(groupArrayArray(arrayMap(h -> (h.timestamp, h.count_int, h.counter_reset_hint), histograms))), groupArrayArray(samples)
FROM timeSeriesSelector({CLICKHOUSE_DATABASE:Identifier}.ts, 'm', toDateTime64('2024-08-20 00:00:00', 3), toDateTime64('2024-08-20 00:05:00', 3), 'samples');

SELECT '-- a time range within one minute: the row of that bucket, sliced';
SELECT arrayMap(h -> (h.timestamp, h.count_int), histograms), samples
FROM timeSeriesSelector({CLICKHOUSE_DATABASE:Identifier}.ts, 'm', toDateTime64('2024-08-20 00:01:00', 3), toDateTime64('2024-08-20 00:01:59', 3), 'samples');

DROP TABLE ts;
