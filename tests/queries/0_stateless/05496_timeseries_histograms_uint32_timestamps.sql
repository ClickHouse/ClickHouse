-- Tags: no-fasttest
-- Tag no-fasttest: `timeSeriesSelector` parses the selector with the PromQL parser, which needs ANTLR4, disabled in the fast-test build.

-- Native histograms in TimeSeries tables whose timestamps are not DateTime64: the `histograms` column and the `histograms.*` outer
-- columns take the type of the timestamps of `samples`, the sink sorts and splits the histograms into buckets with these
-- timestamps, and the selector slices the histogram tuples by them.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';
SET print_pretty_type_names = 0;

DROP TABLE IF EXISTS ts_uint32;
DROP TABLE IF EXISTS ts_datetime;

-- The buckets are 60 seconds long; the recent samples table is disabled because its TTL would drop the old samples.
CREATE TABLE ts_uint32 (samples Array(Tuple(UInt32, Float32))) ENGINE = TimeSeries SETTINGS samples_bucket_step_seconds = 60, recent_samples_ttl_seconds = 0;
CREATE TABLE ts_datetime (samples Array(Tuple(DateTime, Float64))) ENGINE = TimeSeries SETTINGS samples_bucket_step_seconds = 60, recent_samples_ttl_seconds = 0;

SELECT '-- the histograms columns take the type of the timestamps of the table';
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'ts_uint32' AND name IN ('samples', 'histograms.timestamp', 'histograms.count_int') ORDER BY name;
-- The inner samples tables of both TimeSeries tables, the DateTime one first.
SELECT type FROM system.columns WHERE database = currentDatabase() AND table LIKE '.inner_id.samples.%' AND name = 'histograms' ORDER BY type;
SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = 'ts_datetime' AND name IN ('samples', 'histograms.timestamp') ORDER BY name;

SELECT '-- UInt32 timestamps: the histograms are sorted and split into the buckets with the float samples, the time range covers both kinds';
INSERT INTO ts_uint32 (metric_name, tags, samples, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_int)
    VALUES ('m', map('job', 'api'), [(70, 1.5)], [130, 80, 125], [3, 1, 2], [1.5, 0.5, 1], [[(0, 3)], [(0, 1)], [(0, 2)]]);

SELECT s.bucket, s.min_time, s.max_time, s.samples, s.histograms.timestamp, s.histograms.count_int, s.histograms.counter_reset_hint
FROM timeSeriesSamples(ts_uint32) AS s ORDER BY s.bucket;

SELECT '-- the outer columns and the selector';
SELECT histograms.timestamp, histograms.count_int FROM ts_uint32 ORDER BY histograms.timestamp;
SELECT samples, arrayMap(h -> (h.timestamp, h.count_int), histograms) AS h FROM timeSeriesSelector(ts_uint32, 'm', 75, 126, 'samples') ORDER BY h;
SELECT timestamp, value FROM timeSeriesSelector(ts_uint32, 'm', 0, 1000, 'timestamp_and_value');

SELECT '-- the rows of a bucket are merged by timestamp';
INSERT INTO ts_uint32 (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_int)
    VALUES ('m', map('job', 'api'), [140], [4], [2], [[(0, 4)]]);
SELECT histograms.timestamp, histograms.count_int FROM ts_uint32 FINAL ORDER BY histograms.timestamp;
SELECT arrayMap(h -> (h.timestamp, h.count_int), timeSeriesGroupArrayHistograms(histograms)) FROM timeSeriesSelector(ts_uint32, 'm', 0, 1000, 'samples');

SELECT '-- DateTime timestamps';
INSERT INTO ts_datetime (metric_name, tags, samples, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_int)
    VALUES ('m', map('job', 'api'), [(toDateTime(70), 1.5)], [toDateTime(130), toDateTime(80)], [2, 1], [2, 1], [[(0, 2)], [(0, 1)]]);

SELECT s.bucket, s.min_time, s.max_time, s.samples, s.histograms.timestamp, s.histograms.count_int
FROM timeSeriesSamples(ts_datetime) AS s ORDER BY s.bucket;
SELECT arrayMap(h -> (h.timestamp, h.count_int), histograms) AS h FROM timeSeriesSelector(ts_datetime, 'm', toDateTime(75), toDateTime(135), 'samples') ORDER BY h;

DROP TABLE ts_datetime;
DROP TABLE ts_uint32;
