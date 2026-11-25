-- Test: an INSERT into a TimeSeries table rejects a native histogram span with a negative offset where Prometheus does:
-- on any span but the first of an exponential histogram, and on any span of a custom-bucket histogram.
-- The histogram tuple: (timestamp, flags, schema, zero_threshold, count, sum, zero_count, positive_spans, positive_values,
-- negative_spans, negative_values, custom_values, count_int, zero_count_int, positive_values_int, negative_values_int).

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_hist_span_offset;

CREATE TABLE ts_hist_span_offset ENGINE = TimeSeries SETTINGS store_native_histograms = 1;

SELECT '-- exponential schema: a negative offset on a later span is rejected on both sides, even on an empty span';
INSERT INTO ts_hist_span_offset (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(1, 3), 1, 0, 0., 2., 1., 0., [(0, 1), (-1, 1)], [1., 1.], [], [], [], 0, 0, [], [])]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_hist_span_offset (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(1, 3), 1, 0, 0., 2., 1., 0., [], [], [(0, 1), (-1, 1)], [1., 1.], [], 0, 0, [], [])]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_hist_span_offset (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(1, 3), 1, 0, 0., 1., 1., 0., [(0, 1), (-2, 0)], [1.], [], [], [], 0, 0, [], [])]); -- { serverError INCORRECT_DATA }
-- A negative offset on the first span is the index of its first bucket and is accepted.
INSERT INTO ts_hist_span_offset (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(1, 3), 1, 0, 0., 4., 1., 0., [(-3, 1), (0, 1)], [1., 1.], [(-3, 1), (0, 1)], [1., 1.], [], 0, 0, [], [])]);

SELECT '-- custom buckets: a negative offset on any span is rejected';
INSERT INTO ts_hist_span_offset (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(2, 3), 1, -53, 0., 2., 1., 0., [(1, 1), (-1, 1)], [1., 1.], [], [], [1., 2., 3.], 0, 0, [], [])]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_hist_span_offset (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(2, 3), 1, -53, 0., 1., 1., 0., [(-1, 0), (1, 1)], [1.], [], [], [1., 2., 3.], 0, 0, [], [])]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_hist_span_offset (metric_name, tags, histograms) VALUES ('m', map('a', 'b'), [(toDateTime64(2, 3), 1, -53, 0., 2., 1., 0., [(0, 1), (1, 1)], [1., 1.], [], [], [1., 2., 3.], 0, 0, [], [])]);

SELECT '-- the accepted histograms';
SELECT timestamp, `schema`, positive_spans, negative_spans, custom_values
FROM timeSeriesHistograms(ts_hist_span_offset) ORDER BY timestamp;

DROP TABLE ts_hist_span_offset;
