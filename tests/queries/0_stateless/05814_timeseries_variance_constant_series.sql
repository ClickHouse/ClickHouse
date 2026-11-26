-- Stddev/stdvar of a constant series must be exactly 0 when the window spans several buckets.
-- https://github.com/ClickHouse/ClickHouse/issues/122062

SET enable_time_series_aggregate_functions = 1;

-- A sample every 10s, 3 per 30s bucket; `x * 3 / 3 != x` for this value.

SELECT 'recompute';
SELECT arrayAll(x -> x = 0, timeSeriesStdvarToGrid(90, 150, 30, 60)(arrayMap(i -> toUInt32(i * 10), range(1, 31)), arrayMap(i -> 1.5990505637277868, range(1, 31))));
SELECT arrayAll(x -> x = 0, timeSeriesStddevToGrid(90, 150, 30, 60)(arrayMap(i -> toUInt32(i * 10), range(1, 31)), arrayMap(i -> 1.5990505637277868, range(1, 31))));

SELECT 'two-stacks';
SELECT arrayAll(x -> x = 0, timeSeriesStdvarToGrid(300, 600, 30, 300)(arrayMap(i -> toUInt32(i * 10), range(1, 61)), arrayMap(i -> 1.5990505637277868, range(1, 61))));
SELECT arrayAll(x -> x = 0, timeSeriesStddevToGrid(300, 600, 30, 300)(arrayMap(i -> toUInt32(i * 10), range(1, 61)), arrayMap(i -> 1.5990505637277868, range(1, 61))));

SELECT 'huge values'; -- `x * x` overflows Float64
SELECT timeSeriesStdvarToGrid(100, 100, 1, 10)([95]::Array(UInt32), [1e200]::Array(Float64));
SELECT timeSeriesStddevToGrid(100, 100, 1, 10)([95]::Array(UInt32), [-1e200]::Array(Float64));
SELECT timeSeriesStdvarToGrid(90, 150, 30, 60)(arrayMap(i -> toUInt32(i * 10), range(1, 31)), arrayMap(i -> 1e200, range(1, 31)));
SELECT timeSeriesStddevToGrid(300, 600, 30, 300)(arrayMap(i -> toUInt32(i * 10), range(1, 61)), arrayMap(i -> 1e200, range(1, 61)));

SELECT 'table';
DROP TABLE IF EXISTS ts_constant;
CREATE TABLE ts_constant (timestamp DateTime('UTC'), value Float64) ENGINE = MergeTree ORDER BY timestamp;
INSERT INTO ts_constant SELECT toDateTime(number * 10, 'UTC'), 1.5990505637277868 FROM numbers(1, 60);
SELECT arrayAll(x -> x = 0, timeSeriesStddevToGrid(90, 600, 30, 60)(timestamp, value)) FROM ts_constant;
SELECT arrayAll(x -> x = 0, timeSeriesStddevToGrid(300, 600, 30, 300)(timestamp, value)) FROM ts_constant;
DROP TABLE ts_constant;

SELECT 'non-constant'; -- alternating 1 and 3: variance 1
SELECT arrayMap(x -> round(x, 9), timeSeriesStdvarToGrid(90, 150, 30, 60)(arrayMap(i -> toUInt32(i * 10), range(1, 31)), arrayMap(i -> if(i % 2 = 0, 1., 3.), range(1, 31))));
SELECT arrayMap(x -> round(x, 9), timeSeriesStddevToGrid(300, 600, 30, 300)(arrayMap(i -> toUInt32(i * 10), range(1, 61)), arrayMap(i -> if(i % 2 = 0, 1., 3.), range(1, 61))));
