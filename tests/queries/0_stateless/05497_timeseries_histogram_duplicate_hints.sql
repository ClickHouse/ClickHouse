-- Histogram samples with the same timestamp whose payloads are identical except for the counter reset hint: the order of
-- TimeSeriesDuplicateTimestampRule compares the hints last, a gauge marker beats a counter hint, an unknown hint beats a known one,
-- and otherwise the greater hint wins. The sink (INSERT) and the aggregate `timeSeriesGroupArrayHistograms` apply the same order,
-- whatever the input order. A payload difference (here the schema) decides before the hints, but not before the gauge marker.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS hint_pairs;
DROP TABLE IF EXISTS ts;

CREATE TABLE hint_pairs
(
    name String,
    a Array(Tuple(timestamp UInt32, flags UInt8, counter_reset_hint UInt8, schema Int8, zero_threshold Float64,
                  sum Float64, count_int Int64, zero_count_int Int64, count_float Float64, zero_count_float Float64,
                  positive_int Array(Tuple(index Int32, count Int64)), negative_int Array(Tuple(index Int32, count Int64)),
                  positive_float Array(Tuple(index Int32, count Float64)), negative_float Array(Tuple(index Int32, count Float64)),
                  custom_values Array(Float64))),
    b Array(Tuple(timestamp UInt32, flags UInt8, counter_reset_hint UInt8, schema Int8, zero_threshold Float64,
                  sum Float64, count_int Int64, zero_count_int Int64, count_float Float64, zero_count_float Float64,
                  positive_int Array(Tuple(index Int32, count Int64)), negative_int Array(Tuple(index Int32, count Int64)),
                  positive_float Array(Tuple(index Int32, count Float64)), negative_float Array(Tuple(index Int32, count Float64)),
                  custom_values Array(Float64)))
) ENGINE = Memory;

INSERT INTO hint_pairs
SELECT name,
       [(100, 0, a_hint, a_schema, 0., 1., 1, 0, 0., 0., [(0, 1)], [], [], [], [])],
       [(100, 0, b_hint, b_schema, 0., 1., 1, 0, 0., 0., [(0, 1)], [], [], [], [])]
FROM values('name String, a_hint UInt8, a_schema Int8, b_hint UInt8, b_schema Int8',
    ('gauge_vs_no', 3, 0, 2, 0),
    ('unknown_vs_reset', 0, 0, 1, 0),
    ('reset_vs_no', 1, 0, 2, 0),
    ('gauge_vs_no_schema', 3, 0, 2, 1),
    ('unknown_vs_reset_schema', 0, 0, 1, 1));

SELECT '-- the aggregate: two arrays in either order, and one array with both samples';
SELECT name,
       arrayMap(s -> (s.counter_reset_hint, s.schema), arrayReduce('timeSeriesGroupArrayHistograms', [a, b])) AS ab,
       arrayMap(s -> (s.counter_reset_hint, s.schema), arrayReduce('timeSeriesGroupArrayHistograms', [b, a])) AS ba,
       arrayMap(s -> (s.counter_reset_hint, s.schema), arrayReduce('timeSeriesGroupArrayHistograms', [arrayConcat(a, b)])) AS one_array
FROM hint_pairs
ORDER BY name;

SELECT '-- the sink: one sample per series, so the detection of resets leaves the kept hint alone';
CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0;
INSERT INTO ts (metric_name, tags, histograms.timestamp, histograms.counter_reset_hint, histograms.count_int, histograms.sum, histograms.positive_int)
    VALUES
    ('gauge_last', map(), [toDateTime64(100, 3), toDateTime64(100, 3)], [2, 3], [1, 1], [1, 1], [[(0, 1)], [(0, 1)]]),
    ('gauge_first', map(), [toDateTime64(100, 3), toDateTime64(100, 3)], [3, 2], [1, 1], [1, 1], [[(0, 1)], [(0, 1)]]),
    ('unknown_last', map(), [toDateTime64(100, 3), toDateTime64(100, 3)], [1, 0], [1, 1], [1, 1], [[(0, 1)], [(0, 1)]]),
    ('unknown_first', map(), [toDateTime64(100, 3), toDateTime64(100, 3)], [0, 1], [1, 1], [1, 1], [[(0, 1)], [(0, 1)]]),
    ('no_last', map(), [toDateTime64(100, 3), toDateTime64(100, 3)], [1, 2], [1, 1], [1, 1], [[(0, 1)], [(0, 1)]]),
    ('no_first', map(), [toDateTime64(100, 3), toDateTime64(100, 3)], [2, 1], [1, 1], [1, 1], [[(0, 1)], [(0, 1)]]);

SELECT t.metric_name, s.histograms.counter_reset_hint
FROM timeSeriesSamples(ts) AS s JOIN timeSeriesTags(ts) AS t ON s.id = t.id
ORDER BY t.metric_name;

DROP TABLE ts;
DROP TABLE hint_pairs;
