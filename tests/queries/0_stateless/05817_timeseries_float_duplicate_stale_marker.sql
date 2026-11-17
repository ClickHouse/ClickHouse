-- A float sample whose value is the stale marker (the NaN with the bits 0x7FF0000000000002) loses to any other sample with the same
-- timestamp, an ordinary NaN (0x7FF8000000000001, Go's `math.NaN()`) included, whatever the order of the two: in the sink (INSERT),
-- in the aggregate `timeSeriesGroupArray`, which merges the rows of the samples tables, and in the other `timeSeries*` aggregate
-- functions, which share the rule. 0x3FF0000000000000 is 1.

SET allow_experimental_time_series_table = 1;
SET allow_experimental_time_series_aggregate_functions = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;

SELECT '-- timeSeriesGroupArray: two arrays, and one unsorted array, each in either order';
WITH [(100::UInt32, reinterpret(0x7FF0000000000002, 'Float64'))] AS stale,
     [(100::UInt32, reinterpret(0x7FF8000000000001, 'Float64'))] AS other,
     [(200::UInt32, 1.)] AS later
SELECT arrayMap(s -> hex(reinterpretAsUInt64(s.2)), arrayReduce('timeSeriesGroupArray', [stale, other])),
       arrayMap(s -> hex(reinterpretAsUInt64(s.2)), arrayReduce('timeSeriesGroupArray', [other, stale])),
       arrayMap(s -> hex(reinterpretAsUInt64(s.2)), arrayReduce('timeSeriesGroupArray', [arrayConcat(stale, later, other)])),
       arrayMap(s -> hex(reinterpretAsUInt64(s.2)), arrayReduce('timeSeriesGroupArray', [arrayConcat(other, later, stale)]));

SELECT '-- timeSeriesLastToGrid, one of the other aggregate functions: two samples in either order';
WITH reinterpret(0x7FF0000000000002, 'Float64') AS stale, reinterpret(0x7FF8000000000001, 'Float64') AS other
SELECT arrayMap(x -> hex(reinterpretAsUInt64(assumeNotNull(x))),
                timeSeriesLastToGrid(100, 100, 1, 10)([95, 95]::Array(UInt32), [stale, other])),
       arrayMap(x -> hex(reinterpretAsUInt64(assumeNotNull(x))),
                timeSeriesLastToGrid(100, 100, 1, 10)([95, 95]::Array(UInt32), [other, stale]));

SELECT '-- the sink: both orders within one INSERT';
CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0;
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('stale_first', map(), [(toDateTime64(100, 3), reinterpret(0x7FF0000000000002, 'Float64')),
                            (toDateTime64(100, 3), reinterpret(0x7FF8000000000001, 'Float64'))]),
    ('stale_last', map(), [(toDateTime64(100, 3), reinterpret(0x7FF8000000000001, 'Float64')),
                           (toDateTime64(100, 3), reinterpret(0x7FF0000000000002, 'Float64'))]);

SELECT t.metric_name, arrayMap(x -> hex(reinterpretAsUInt64(x)), s.samples.value)
FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts) AS s JOIN timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts) AS t ON s.id = t.id
ORDER BY t.metric_name;

DROP TABLE ts;
