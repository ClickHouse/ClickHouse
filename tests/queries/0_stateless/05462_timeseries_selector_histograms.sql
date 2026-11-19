-- Tags: no-fasttest
-- Tag no-fasttest: `timeSeriesSelector` parses the selector with the PromQL parser, which needs ANTLR4, disabled in the fast-test build.

-- `timeSeriesSelector` over a TimeSeries table storing native histogram samples: `return_columns` names the returned columns,
-- 'samples' (the float samples), 'histograms' (the histogram samples, the stored tuples of a row of the samples table cut to the
-- time range) or 'samples_and_histograms' (both, of the same rows), one row per series and bucket. 'timestamp_and_value' returns
-- the float samples only. PromQL over such a table is covered by 05493_timeseries_selector_histogram_presence.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';
SET print_pretty_type_names = 0;

DROP TABLE IF EXISTS ts;
DROP TABLE IF EXISTS ts_v11;
DROP TABLE IF EXISTS ts_recent;

-- The buckets are 60 seconds long; the recent samples table is disabled because its TTL would drop the old samples.
CREATE TABLE ts ENGINE = TimeSeries SETTINGS samples_bucket_step_seconds = 60, recent_samples_ttl_seconds = 0;
CREATE TABLE ts_v11 ENGINE = TimeSeries SETTINGS version = 11, recent_samples_ttl_seconds = 0;

SELECT '-- the returned columns are named by return_columns; a table of an earlier version has no histograms to return';
DESCRIBE timeSeriesSelector(ts, 'm', 0, 1, 'samples');
DESCRIBE timeSeriesSelector(ts, 'm', 0, 1, 'histograms');
DESCRIBE timeSeriesSelector(ts, 'm', 0, 1, 'samples_and_histograms');
DESCRIBE timeSeriesSelector(ts, 'm', 0, 1, 'timestamp_and_value');
DESCRIBE timeSeriesSelector(ts_v11, 'm', 0, 1, 'samples');
DESCRIBE timeSeriesSelector(ts_v11, 'm', 0, 1, 'histograms'); -- { serverError BAD_ARGUMENTS }
DESCRIBE timeSeriesSelector(ts_v11, 'm', 0, 1, 'samples_and_histograms'); -- { serverError BAD_ARGUMENTS }
DESCRIBE timeSeriesSelector(ts, 'm', 0, 1, 'floats'); -- { serverError BAD_ARGUMENTS }

-- A float-only series.
INSERT INTO ts (metric_name, tags, samples) VALUES ('m_float', map('job', 'api'), [(toDateTime64(100, 3), 1.5), (toDateTime64(110, 3), 2.5)]);
-- A histogram-only series: an integer histogram, then a float histogram, in one bucket.
INSERT INTO ts (metric_name, tags, histograms)
SELECT 'm_hist', map('job', 'api'),
    CAST([(toDateTime64(100, 3), false, 'not_reset', 3, 0.001, 12.5, 10, 2, [(-2, 3), (-1, 2), (1, 3)], [(0, 0)], 0, 0, [], []),
          (toDateTime64(110, 3), true, 'gauge', 0, 0, 3.25, 0, 0, [], [], 5.75, 0.5, [(0, 2.5), (1, 2.5)], [(1, 0.25)])],
         'Array(Tuple(timestamp DateTime64(3), is_float Bool, counter_reset_hint String, schema Int8, zero_threshold Float64, sum Float64,
                      count_int Int64, zero_count_int Int64, positive_int Array(Tuple(Int32, Int64)), negative_int Array(Tuple(Int32, Int64)),
                      count_float Float64, zero_count_float Float64, positive_float Array(Tuple(Int32, Float64)), negative_float Array(Tuple(Int32, Float64))))');
-- A float sample and a histogram sample of one series in one bucket.
INSERT INTO ts (metric_name, tags, samples, histograms)
SELECT 'm_mixed', map('job', 'api'), [(toDateTime64(100, 3), 7.)], CAST([(toDateTime64(110, 3), 5, 2.5, [(0, 5)])], 'Array(Tuple(timestamp DateTime64(3), count_int Int64, sum Float64, positive_int Array(Tuple(Int32, Int64))))');
-- Histograms in three buckets, a float sample in the first one.
INSERT INTO ts (metric_name, tags, samples, histograms)
SELECT 'm_buckets', map('job', 'api'), [(toDateTime64(65, 3), 1.)],
    CAST([(toDateTime64(100, 3), 1, 0.5, [(0, 1)]), (toDateTime64(130, 3), 2, 1.5, [(0, 2)]), (toDateTime64(200, 3), 3, 2.5, [(0, 3)])], 'Array(Tuple(timestamp DateTime64(3), count_int Int64, sum Float64, positive_int Array(Tuple(Int32, Int64))))');
-- Custom buckets: three bounds define four buckets, the last one up to +Inf.
INSERT INTO ts (metric_name, tags, histograms)
SELECT 'm_nhcb', map('job', 'api'),
    CAST([(toDateTime64(100, 3), -53, 15, 4.2, [(0, 1), (1, 2), (2, 4), (3, 8)], [0.1, 0.5, 1])],
         'Array(Tuple(timestamp DateTime64(3), schema Int8, count_int Int64, sum Float64, positive_int Array(Tuple(Int32, Int64)), custom_values Array(Float64)))');
-- An integer count which Float64 can't hold exactly, and a stale marker (the NaN with the payload 0x7FF0000000000002 in `sum`).
INSERT INTO ts (metric_name, tags, histograms)
SELECT 'm_big', map('job', 'api'), CAST([(toDateTime64(100, 3), 9007199254740993, 1, [(0, 9007199254740993)])], 'Array(Tuple(timestamp DateTime64(3), count_int Int64, sum Float64, positive_int Array(Tuple(Int32, Int64))))');
INSERT INTO ts (metric_name, tags, histograms)
SELECT 'm_stale', map('job', 'api'),
    CAST([(toDateTime64(100, 3), reinterpretAsFloat64(toUInt64(9218868437227405314)))], 'Array(Tuple(timestamp DateTime64(3), sum Float64))');

SELECT '-- the histogram samples are returned as stored next to the float samples of the row';
SELECT samples, histograms FROM timeSeriesSelector(ts, 'm_hist', 0, 1000, 'samples_and_histograms');
SELECT samples, length(histograms), arrayMap(h -> h.count_int, histograms) FROM timeSeriesSelector(ts, 'm_mixed', 0, 1000, 'samples_and_histograms');

SELECT '-- custom buckets, exact integer counts, the stale marker bit-exact';
SELECT h.schema, h.custom_values, h.positive_int FROM timeSeriesSelector(ts, 'm_nhcb', 0, 1000, 'histograms') ARRAY JOIN histograms AS h;
SELECT h.count_int, h.positive_int FROM timeSeriesSelector(ts, 'm_big', 0, 1000, 'histograms') ARRAY JOIN histograms AS h;
SELECT hex(reinterpretAsUInt64(h.sum)), h.count_int FROM timeSeriesSelector(ts, 'm_stale', 0, 1000, 'histograms') ARRAY JOIN histograms AS h;

SELECT '-- the time range cuts both arrays; for a range shorter than a bucket, a row with nothing of the returned kinds in it is dropped';
SELECT length(samples), length(histograms) FROM timeSeriesSelector(ts, 'm_buckets', 0, 1000, 'samples_and_histograms') ORDER BY ALL;
SELECT length(samples), length(histograms) FROM timeSeriesSelector(ts, 'm_buckets', 70, 150, 'samples_and_histograms') ORDER BY ALL;
-- The bucket 60 has the samples 65 and 100, so its row matches [70, 90] by `min_time` and `max_time` but has nothing in it.
SELECT count() FROM timeSeriesSelector(ts, 'm_buckets', 70, 90, 'samples_and_histograms');
SELECT count() FROM timeSeriesSelector(ts, 'm_buckets', 70, 100, 'samples_and_histograms');
-- [70, 150] is longer than a bucket, so the rows of the buckets 60 and 120 are returned for their histogram samples also with
-- 'samples', with empty arrays: the float sample 65 is out of the range.
SELECT count() FROM timeSeriesSelector(ts, 'm_buckets', 70, 150, 'samples');
SELECT count() FROM timeSeriesSelector(ts, 'm_buckets', 70, 150, 'histograms');

SELECT '-- the rows of a series are merged with timeSeriesGroupArray';
SELECT timeSeriesGroupArray(samples), arrayMap(h -> (h.timestamp, h.count_int), timeSeriesGroupArray(histograms))
FROM timeSeriesSelector(ts, 'm_buckets', 0, 1000, 'samples_and_histograms') GROUP BY id;

SELECT '-- a range of at least a bucket returns the rows with samples of any kind, an empty array for a returned kind without samples';
SELECT count(), sum(length(samples)) FROM timeSeriesSelector(ts, 'm_hist', 0, 1000, 'samples');
SELECT count(), sum(length(histograms)) FROM timeSeriesSelector(ts, 'm_hist', 0, 1000, 'histograms');
SELECT count(), sum(length(samples)), sum(length(histograms)) FROM timeSeriesSelector(ts, 'm_hist', 0, 1000, 'samples_and_histograms');
SELECT count(), sum(length(histograms)) FROM timeSeriesSelector(ts, 'm_float', 0, 1000, 'histograms');
SELECT count(), sum(length(samples)), sum(length(histograms)) FROM timeSeriesSelector(ts, 'm_float', 0, 1000, 'samples_and_histograms');

SELECT '-- each mode reads only its columns';
-- 'samples' reads no histograms, 'histograms' no float samples (cast to `Array(Tuple(DateTime64(3), Float64))`),
-- 'samples_and_histograms' both (the type of the histograms names `count_int`).
SELECT plan NOT LIKE '%histograms%'
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN actions = 1 SELECT id, samples FROM timeSeriesSelector(ts, 'm_mixed', 0, 1000, 'samples')));
SELECT plan LIKE '%count_int%' AND plan NOT LIKE '%Array(Tuple(DateTime64(3), Float64))%'
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN actions = 1 SELECT id, histograms FROM timeSeriesSelector(ts, 'm_mixed', 0, 1000, 'histograms')));
SELECT plan LIKE '%count_int%' AND plan LIKE '%Array(Tuple(DateTime64(3), Float64))%'
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN actions = 1 SELECT id, samples, histograms FROM timeSeriesSelector(ts, 'm_mixed', 0, 1000, 'samples_and_histograms')));

SELECT '-- timestamp_and_value returns the float samples only';
SELECT timestamp, value FROM timeSeriesSelector(ts, 'm_mixed', 0, 1000, 'timestamp_and_value');
-- A histogram-only series makes no rows.
SELECT count() FROM timeSeriesSelector(ts, 'm_hist', 0, 1000, 'timestamp_and_value');

SELECT '-- the recent samples table returns the histograms too';
-- TTL 10 days, the samples are inserted close to now(), so the background TTL cannot drop them during the test.
CREATE TABLE ts_recent ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000;
INSERT INTO ts_recent (metric_name, tags, samples, histograms)
SELECT 'm', map('job', 'api'), [(now64(3) - INTERVAL 3 MINUTE, 1.)],
    CAST([(now64(3) - INTERVAL 2 MINUTE, 1, [(0, 1)]), (now64(3) - INTERVAL 1 MINUTE, 2, [(0, 2)])],
         'Array(Tuple(timestamp DateTime64(3), count_int Int64, positive_int Array(Tuple(Int32, Int64))))');
SET time_series_prefer_recent_samples_table = 1;
SELECT sum(length(samples)), sum(length(histograms)) FROM timeSeriesSelector(ts_recent, 'm', now() - INTERVAL 10 MINUTE, now(), 'samples_and_histograms');

DROP TABLE ts_recent;
DROP TABLE ts_v11;
DROP TABLE ts;
