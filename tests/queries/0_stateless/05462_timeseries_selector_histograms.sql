-- Tags: no-fasttest
-- Tag no-fasttest: `timeSeriesSelector` parses the selector with the PromQL parser, which needs ANTLR4, disabled in the fast-test build.

-- `timeSeriesSelector` over a TimeSeries table storing native histogram samples returns them in the column `histograms` next to
-- `samples` with `return_columns = 'samples'`: the stored tuples of a row of the samples table cut to the time range, one row per
-- series and bucket. Both arrays are read, and a row is returned when either array has samples in the range, regardless of projection.
-- `'timestamp_and_value'` returns the float samples only. PromQL over such a table is covered by 05493_timeseries_selector_histogram_presence.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';
SET print_pretty_type_names = 0;

DROP TABLE IF EXISTS ts;
DROP TABLE IF EXISTS ts_v11;
DROP TABLE IF EXISTS ts_recent;

-- The buckets are 60 seconds long; the recent samples table is disabled because its TTL would drop the old samples.
CREATE TABLE ts ENGINE = TimeSeries SETTINGS samples_bucket_step_seconds = 60, recent_samples_ttl_seconds = 0;
CREATE TABLE ts_v11 ENGINE = TimeSeries SETTINGS version = 11, recent_samples_ttl_seconds = 0;

SELECT '-- the returned columns: histograms with return_columns = samples on a table storing them, the float samples only otherwise';
DESCRIBE timeSeriesSelector(ts, 'm', 0, 1, 'samples');
DESCRIBE timeSeriesSelector(ts, 'm', 0, 1, 'timestamp_and_value');
DESCRIBE timeSeriesSelector(ts_v11, 'm', 0, 1, 'samples');

-- A float-only series.
INSERT INTO ts (metric_name, tags, samples) VALUES ('m_float', map('job', 'api'), [(toDateTime64(100, 3), 1.5), (toDateTime64(110, 3), 2.5)]);
-- A histogram-only series: an integer histogram, then a float histogram, in one bucket.
INSERT INTO ts (metric_name, tags, histograms.timestamp, histograms.flags, histograms.counter_reset_hint, histograms.schema, histograms.zero_threshold, histograms.sum,
                histograms.count_int, histograms.zero_count_int, histograms.positive_int, histograms.negative_int,
                histograms.count_float, histograms.zero_count_float, histograms.positive_float, histograms.negative_float)
    VALUES ('m_hist', map('job', 'api'), [toDateTime64(100, 3), toDateTime64(110, 3)], [0, 1], [2, 3], [3, 0], [0.001, 0], [12.5, 3.25],
            [10, 0], [2, 0], [[(-2, 3), (-1, 2), (1, 3)], []], [[(0, 0)], []],
            [0, 5.75], [0, 0.5], [[], [(0, 2.5), (1, 2.5)]], [[], [(1, 0.25)]]);
-- A float sample and a histogram sample of one series in one bucket.
INSERT INTO ts (metric_name, tags, samples, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_int)
    VALUES ('m_mixed', map('job', 'api'), [(toDateTime64(100, 3), 7.)], [toDateTime64(110, 3)], [5], [2.5], [[(0, 5)]]);
-- Histograms in three buckets, a float sample in the first one.
INSERT INTO ts (metric_name, tags, samples, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_int)
    VALUES ('m_buckets', map('job', 'api'), [(toDateTime64(65, 3), 1.)],
            [toDateTime64(100, 3), toDateTime64(130, 3), toDateTime64(200, 3)], [1, 2, 3], [0.5, 1.5, 2.5], [[(0, 1)], [(0, 2)], [(0, 3)]]);
-- Custom buckets: three bounds define four buckets, the last one up to +Inf.
INSERT INTO ts (metric_name, tags, histograms.timestamp, histograms.schema, histograms.count_int, histograms.sum, histograms.positive_int, histograms.custom_values)
    VALUES ('m_nhcb', map('job', 'api'), [toDateTime64(100, 3)], [-53], [15], [4.2], [[(0, 1), (1, 2), (2, 4), (3, 8)]], [[0.1, 0.5, 1]]);
-- An integer count which Float64 can't hold exactly, and a stale marker (the NaN with the payload 0x7FF0000000000002 in `sum`).
INSERT INTO ts (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_int)
    VALUES ('m_big', map('job', 'api'), [toDateTime64(100, 3)], [9007199254740993], [1], [[(0, 9007199254740993)]]);
INSERT INTO ts (metric_name, tags, histograms.timestamp, histograms.sum)
    VALUES ('m_stale', map('job', 'api'), [toDateTime64(100, 3)], [reinterpretAsFloat64(toUInt64(9218868437227405314))]);

SELECT '-- the histogram samples are returned as stored next to the float samples of the row';
SELECT samples, histograms FROM timeSeriesSelector(ts, 'm_hist', 0, 1000, 'samples');
SELECT samples, length(histograms), arrayMap(h -> h.count_int, histograms) FROM timeSeriesSelector(ts, 'm_mixed', 0, 1000, 'samples');

SELECT '-- custom buckets, exact integer counts, the stale marker bit-exact';
SELECT h.schema, h.custom_values, h.positive_int FROM timeSeriesSelector(ts, 'm_nhcb', 0, 1000, 'samples') ARRAY JOIN histograms AS h;
SELECT h.count_int, h.positive_int FROM timeSeriesSelector(ts, 'm_big', 0, 1000, 'samples') ARRAY JOIN histograms AS h;
SELECT hex(reinterpretAsUInt64(h.sum)), h.count_int FROM timeSeriesSelector(ts, 'm_stale', 0, 1000, 'samples') ARRAY JOIN histograms AS h;

SELECT '-- the time range cuts both arrays; a row with nothing of either kind in the range is dropped';
SELECT length(samples), length(histograms) FROM timeSeriesSelector(ts, 'm_buckets', 0, 1000, 'samples') ORDER BY ALL;
SELECT length(samples), length(histograms) FROM timeSeriesSelector(ts, 'm_buckets', 70, 150, 'samples') ORDER BY ALL;
-- The bucket 60 has the samples 65 and 100, so its row matches [70, 90] by `min_time` and `max_time` but has nothing in it.
SELECT count() FROM timeSeriesSelector(ts, 'm_buckets', 70, 90, 'samples');
SELECT count() FROM timeSeriesSelector(ts, 'm_buckets', 70, 100, 'samples');

SELECT '-- the rows of a series are merged with timeSeriesGroupArrayHistograms';
SELECT timeSeriesGroupArray(samples), arrayMap(h -> (h.timestamp, h.count_int), timeSeriesGroupArrayHistograms(histograms))
FROM timeSeriesSelector(ts, 'm_buckets', 0, 1000, 'samples') GROUP BY id;

SELECT '-- the same rows are returned regardless of which arrays are requested';
SELECT count(), sum(length(samples)) FROM timeSeriesSelector(ts, 'm_hist', 0, 1000, 'samples');
SELECT count(), sum(length(histograms)) FROM timeSeriesSelector(ts, 'm_hist', 0, 1000, 'samples');
SELECT count() FROM timeSeriesSelector(ts, 'm_hist', 0, 1000, 'samples');
SELECT count(), sum(length(histograms)) FROM timeSeriesSelector(ts, 'm_float', 0, 1000, 'samples');
SELECT count(), sum(length(samples)) FROM timeSeriesSelector(ts, 'm_float', 0, 1000, 'samples');
SELECT histograms FROM timeSeriesSelector(ts, 'm_float', 0, 1000, 'samples');

-- A kind which isn't requested is read through its `timestamp` subcolumn for the row filter only, so histogram-only rows are
-- preserved without reading the histogram payload (the type of the payload names `count_int`, the timestamps don't).
SELECT plan LIKE '%histograms.timestamp%' AND plan NOT LIKE '%count_int%'
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN actions = 1 SELECT id, samples FROM timeSeriesSelector(ts, 'm_mixed', 0, 1000, 'samples')));
SELECT plan LIKE '%samples.timestamp%' AND plan LIKE '%count_int%'
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN actions = 1 SELECT id, histograms FROM timeSeriesSelector(ts, 'm_mixed', 0, 1000, 'samples')));
SELECT plan NOT LIKE '%.timestamp%' AND plan LIKE '%count_int%' AND plan LIKE '%timeSeriesSliceSortedArray%'
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN actions = 1 SELECT id, samples, histograms FROM timeSeriesSelector(ts, 'm_mixed', 0, 1000, 'samples')));

SELECT '-- timestamp_and_value returns the float samples only';
SELECT timestamp, value FROM timeSeriesSelector(ts, 'm_mixed', 0, 1000, 'timestamp_and_value');
-- A histogram-only series makes no rows.
SELECT count() FROM timeSeriesSelector(ts, 'm_hist', 0, 1000, 'timestamp_and_value');

SELECT '-- the recent samples table returns the histograms too';
-- TTL 10 days, the samples are inserted close to now(), so the background TTL cannot drop them during the test.
CREATE TABLE ts_recent ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000;
INSERT INTO ts_recent (metric_name, tags, samples, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_int)
    VALUES ('m', map('job', 'api'), [(now64(3) - INTERVAL 3 MINUTE, 1.)], [now64(3) - INTERVAL 2 MINUTE, now64(3) - INTERVAL 1 MINUTE], [1, 2], [0, 0], [[(0, 1)], [(0, 2)]]);
SET time_series_prefer_recent_samples_table = 1;
SELECT sum(length(samples)), sum(length(histograms)) FROM timeSeriesSelector(ts_recent, 'm', now() - INTERVAL 10 MINUTE, now(), 'samples');

DROP TABLE ts_recent;
DROP TABLE ts_v11;
DROP TABLE ts;
