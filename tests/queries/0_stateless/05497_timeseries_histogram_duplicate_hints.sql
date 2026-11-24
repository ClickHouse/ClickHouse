-- Histogram samples with the same timestamp whose payloads are identical except for the counter reset hint: the order of
-- TimeSeriesDuplicateTimestampRule doesn't compare the counter hints, a gauge marker beats a counter hint, and the kept sample, the
-- first of its array, has an unknown hint unless it is a gauge. The sink (INSERT) and the aggregate `timeSeriesGroupArray` apply the
-- same rule, whatever the input order. A payload difference (here the schema) decides the winner, but not before the gauge marker.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS hint_pairs;
DROP TABLE IF EXISTS ts;

CREATE TABLE hint_pairs
(
    name String,
    a Array(Tuple(timestamp UInt32, is_float Bool, counter_reset_hint Enum8('unknown' = 0, 'reset' = 1, 'not_reset' = 2, 'gauge' = 3),
                  schema Int8, zero_threshold Float64,
                  sum Float64, count_int Int64, zero_count_int Int64, count_float Float64, zero_count_float Float64,
                  positive_int Array(Tuple(index Int32, count Int64)), negative_int Array(Tuple(index Int32, count Int64)),
                  positive_float Array(Tuple(index Int32, count Float64)), negative_float Array(Tuple(index Int32, count Float64)),
                  custom_values Array(Float64))),
    b Array(Tuple(timestamp UInt32, is_float Bool, counter_reset_hint Enum8('unknown' = 0, 'reset' = 1, 'not_reset' = 2, 'gauge' = 3),
                  schema Int8, zero_threshold Float64,
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
    ('gauge_vs_unknown', 3, 0, 0, 0),
    ('unknown_vs_unknown', 0, 0, 0, 0),
    ('gauge_vs_unknown_schema', 3, 0, 0, 1),
    ('unknown_vs_unknown_schema', 0, 0, 0, 1));

SELECT '-- the aggregate: two arrays in either order, and one array with both samples';
SELECT name,
       arrayMap(s -> (s.counter_reset_hint, s.schema), arrayReduce('timeSeriesGroupArray', [a, b])) AS ab,
       arrayMap(s -> (s.counter_reset_hint, s.schema), arrayReduce('timeSeriesGroupArray', [b, a])) AS ba,
       arrayMap(s -> (s.counter_reset_hint, s.schema), arrayReduce('timeSeriesGroupArray', [arrayConcat(a, b)])) AS one_array
FROM hint_pairs
ORDER BY name;

SELECT '-- the sink: one sample per series after the deduplication, the first of its row, which gets an unknown hint unless it is a gauge';
CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0;
-- Two samples of every series with the timestamp 100 and the same payload, with the hints of `hints`.
INSERT INTO ts (metric_name, tags, histograms)
SELECT name, map(),
       CAST(arrayMap(hint -> (toDateTime64(100, 3), hint, 1, 1, [(0, 1)]), hints),
            'Array(Tuple(timestamp DateTime64(3), counter_reset_hint UInt8, count_int Int64, sum Float64, positive_int Array(Tuple(Int32, Int64))))')
FROM values('name String, hints Array(UInt8)',
    ('gauge_last', [2, 3]),
    ('gauge_first', [3, 2]),
    ('unknown_last', [1, 0]),
    ('unknown_first', [0, 1]),
    ('no_last', [1, 2]),
    ('no_first', [2, 1]),
    ('no_both', [2, 2]));

SELECT t.metric_name, s.histograms.counter_reset_hint
FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts) AS s JOIN timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts) AS t ON s.id = t.id
ORDER BY t.metric_name;

DROP TABLE ts;
DROP TABLE hint_pairs;
