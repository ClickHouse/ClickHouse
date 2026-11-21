-- A histogram whose `sum` is the stale marker (the NaN with the bits 0x7FF0000000000002) loses to any other histogram with the same
-- timestamp, whatever the order of the two and the other elements of the stale marker, which are ignored: to one with an ordinary
-- NaN `sum` (0x7FF8000000000001, Go's `math.NaN()`) and to one with a smaller count than the stale marker's. The aggregate
-- `timeSeriesGroupArray`, which merges the rows of the samples tables, and the sink (INSERT) apply the same rule. 0x3FF0000000000000 is 1.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS stale_pairs;
DROP TABLE IF EXISTS ts;

CREATE TABLE stale_pairs
(
    name String,
    stale Array(Tuple(timestamp UInt32, is_float Bool, counter_reset_hint Enum8('unknown' = 0, 'reset' = 1, 'not_reset' = 2, 'gauge' = 3),
                      schema Int8, zero_threshold Float64,
                      sum Float64, count_int Int64, zero_count_int Int64, count_float Float64, zero_count_float Float64,
                      positive_int Array(Tuple(index Int32, count Int64)), negative_int Array(Tuple(index Int32, count Int64)),
                      positive_float Array(Tuple(index Int32, count Float64)), negative_float Array(Tuple(index Int32, count Float64)),
                      custom_values Array(Float64))),
    other Array(Tuple(timestamp UInt32, is_float Bool, counter_reset_hint Enum8('unknown' = 0, 'reset' = 1, 'not_reset' = 2, 'gauge' = 3),
                      schema Int8, zero_threshold Float64,
                      sum Float64, count_int Int64, zero_count_int Int64, count_float Float64, zero_count_float Float64,
                      positive_int Array(Tuple(index Int32, count Int64)), negative_int Array(Tuple(index Int32, count Int64)),
                      positive_float Array(Tuple(index Int32, count Float64)), negative_float Array(Tuple(index Int32, count Float64)),
                      custom_values Array(Float64)))
) ENGINE = Memory;

INSERT INTO stale_pairs
SELECT name,
       [(100, 0, 0, 0, 0., reinterpret(0x7FF0000000000002, 'Float64'), stale_count, 0, 0., 0.,
         if(stale_count = 0, [], [(0, stale_count)]), [], [], [], [])],
       [(100, 0, 0, 0, 0., reinterpret(other_sum, 'Float64'), other_count, 0, 0., 0.,
         if(other_count = 0, [], [(0, other_count)]), [], [], [], [])]
FROM values('name String, stale_count Int64, other_sum UInt64, other_count Int64',
    ('ordinary_nan', 0, 0x7FF8000000000001, 0),
    ('smaller_count', 5, 0x3FF0000000000000, 3));

SELECT '-- the aggregate: two arrays and one array, each in either order';
SELECT name,
       arrayMap(h -> (hex(reinterpretAsUInt64(h.sum)), h.count_int), arrayReduce('timeSeriesGroupArray', [stale, other])),
       arrayMap(h -> (hex(reinterpretAsUInt64(h.sum)), h.count_int), arrayReduce('timeSeriesGroupArray', [other, stale])),
       arrayMap(h -> (hex(reinterpretAsUInt64(h.sum)), h.count_int), arrayReduce('timeSeriesGroupArray', [arrayConcat(stale, other)])),
       arrayMap(h -> (hex(reinterpretAsUInt64(h.sum)), h.count_int), arrayReduce('timeSeriesGroupArray', [arrayConcat(other, stale)]))
FROM stale_pairs
ORDER BY name;

SELECT '-- the sink: both orders within one INSERT';
CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0;
-- The same pairs as above, in the array of one series each; the timestamps 100 become `DateTime64(3)`.
INSERT INTO ts (metric_name, tags, histograms)
SELECT concat(name, '_stale_first'), map(), arrayConcat(stale, other) FROM stale_pairs
UNION ALL
SELECT concat(name, '_stale_last'), map(), arrayConcat(other, stale) FROM stale_pairs;

SELECT t.metric_name, arrayMap(x -> hex(reinterpretAsUInt64(x)), s.histograms.sum), s.histograms.count_int
FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts) AS s JOIN timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts) AS t ON s.id = t.id
ORDER BY t.metric_name;

DROP TABLE ts;
DROP TABLE stale_pairs;
