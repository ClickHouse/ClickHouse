-- Tags: no-fasttest
-- Tag no-fasttest: `timeSeriesSelector` parses the selector with the PromQL parser, which needs ANTLR4, disabled in the fast-test build.

-- Native histograms in a TimeSeries table whose timestamps are `UInt32`, not `DateTime64`: the `histograms` columns take the type of
-- the timestamps of `samples`, the sink sorts and splits the histograms into buckets with these timestamps, the rows of a bucket are
-- merged by them, and the selector slices the histogram tuples by them.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_uint32;

-- The buckets are 60 seconds long; the recent samples table is disabled because its TTL would drop the old samples.
CREATE TABLE ts_uint32 (samples Array(Tuple(UInt32, Float32))) ENGINE = TimeSeries SETTINGS samples_bucket_step_seconds = 60, recent_samples_ttl_seconds = 0;

SELECT '-- UInt32 timestamps: the histograms are sorted and split into the buckets with the float samples, the time range covers both kinds';
INSERT INTO ts_uint32 (metric_name, tags, samples, histograms)
SELECT 'm', map('job', 'api'), [(70, 1.5)], CAST([(130, 3, 1.5, [(0, 3)]), (80, 1, 0.5, [(0, 1)]), (125, 2, 1, [(0, 2)])], 'Array(Tuple(timestamp UInt32, count_int Int64, sum Float64, positive_int Array(Tuple(Int32, Int64))))');

SELECT s.bucket, s.min_time, s.max_time, s.samples, s.histograms.timestamp, s.histograms.count_int, s.histograms.counter_reset_hint
FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts_uint32) AS s ORDER BY s.bucket;

SELECT '-- the outer columns and the selector';
SELECT histograms.timestamp, histograms.count_int FROM ts_uint32 ORDER BY histograms.timestamp;
SELECT samples, arrayMap(h -> (h.timestamp, h.count_int), histograms) AS h FROM timeSeriesSelector({CLICKHOUSE_DATABASE:Identifier}.ts_uint32, 'm', 75, 126, 'samples_and_histograms') ORDER BY h;
SELECT timestamp, value FROM timeSeriesSelector({CLICKHOUSE_DATABASE:Identifier}.ts_uint32, 'm', 0, 1000, 'timestamp_and_value');

SELECT '-- the rows of a bucket are merged by timestamp';
INSERT INTO ts_uint32 (metric_name, tags, histograms) SELECT 'm', map('job', 'api'), CAST([(140, 4, 2, [(0, 4)])], 'Array(Tuple(timestamp UInt32, count_int Int64, sum Float64, positive_int Array(Tuple(Int32, Int64))))');
SELECT histograms.timestamp, histograms.count_int FROM ts_uint32 FINAL ORDER BY histograms.timestamp;
SELECT arrayMap(h -> (h.timestamp, h.count_int), timeSeriesGroupArray(histograms)) FROM timeSeriesSelector({CLICKHOUSE_DATABASE:Identifier}.ts_uint32, 'm', 0, 1000, 'histograms');

DROP TABLE ts_uint32;
