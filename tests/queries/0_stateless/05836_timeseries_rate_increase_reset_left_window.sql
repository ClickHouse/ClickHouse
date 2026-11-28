-- A counter reset that left the window must not change the results of later grid points.

SET enable_time_series_aggregate_functions = 1;

-- The reset 1e16 in (0, 10] must not round the reset 5 in (10, 20]: grid point 30 sees only (10, 30].
WITH [1, 2, 11, 12, 13, 21, 22]::Array(DateTime) AS ts, [1e16, 0, 5, 3, 4, 6, 7]::Array(Float64) AS v
SELECT
    'increase',
    timeSeriesIncreaseToGrid(10, 30, 10, 20)(ts, v) AS all_samples,
    timeSeriesIncreaseToGrid(10, 30, 10, 20)(arraySlice(ts, 3), arraySlice(v, 3)) AS without_first_bucket,
    timeSeriesIncreaseToGrid(30, 30, 10, 20)(ts, v) AS single_point,
    toString(all_samples[3]) = toString(without_first_bucket[3]),
    toString(all_samples[3]) = toString(single_point[1]);

WITH [1, 2, 11, 12, 13, 21, 22]::Array(DateTime) AS ts, [1e16, 0, 5, 3, 4, 6, 7]::Array(Float64) AS v
SELECT
    'rate',
    timeSeriesRateToGrid(10, 30, 10, 20)(ts, v) AS all_samples,
    timeSeriesRateToGrid(10, 30, 10, 20)(arraySlice(ts, 3), arraySlice(v, 3)) AS without_first_bucket,
    timeSeriesRateToGrid(30, 30, 10, 20)(ts, v) AS single_point,
    toString(all_samples[3]) = toString(without_first_bucket[3]),
    toString(all_samples[3]) = toString(single_point[1]);

-- The same with the reset 1e16 at the boundary between (0, 10] and (10, 20].
WITH [1, 2, 11, 12, 21, 22]::Array(DateTime) AS ts, [1, 1e16, 5, 3, 6, 7]::Array(Float64) AS v
SELECT
    'boundary',
    timeSeriesIncreaseToGrid(10, 30, 10, 20)(ts, v) AS all_samples,
    timeSeriesIncreaseToGrid(10, 30, 10, 20)(arraySlice(ts, 3), arraySlice(v, 3)) AS without_first_bucket,
    toString(all_samples[3]) = toString(without_first_bucket[3]);

-- The same with the window equal to the step.
WITH [1, 2, 11, 12, 13]::Array(DateTime) AS ts, [1e16, 0, 5, 3, 4]::Array(Float64) AS v
SELECT
    'window = step',
    timeSeriesIncreaseToGrid(10, 20, 10, 10)(ts, v) AS all_samples,
    timeSeriesIncreaseToGrid(10, 20, 10, 10)(arraySlice(ts, 3), arraySlice(v, 3)) AS without_first_bucket,
    toString(all_samples[2]) = toString(without_first_bucket[2]);

-- A sample +Inf, or resets that overflow Float64, must not affect the windows (20, 40], (30, 50] and (40, 60].
WITH
    [1, 2, 11, 12, 13, 21, 22, 31, 32, 33, 41, 42, 51, 52]::Array(DateTime) AS ts,
    [inf, 0, 5, 3, 4, 6, 7, 8, 2, 3, 4, 5, 6, 7]::Array(Float64) AS v
SELECT
    'inf increase',
    timeSeriesIncreaseToGrid(10, 60, 10, 20)(ts, v) AS all_samples,
    timeSeriesIncreaseToGrid(10, 60, 10, 20)(arraySlice(ts, 6), arraySlice(v, 6)) AS after_20,
    toString(arraySlice(all_samples, 4)) = toString(arraySlice(after_20, 4));

WITH
    [1, 2, 11, 12, 13, 21, 22, 31, 32, 33, 41, 42, 51, 52]::Array(DateTime) AS ts,
    [inf, 0, 5, 3, 4, 6, 7, 8, 2, 3, 4, 5, 6, 7]::Array(Float64) AS v
SELECT
    'inf rate',
    timeSeriesRateToGrid(10, 60, 10, 20)(ts, v) AS all_samples,
    timeSeriesRateToGrid(10, 60, 10, 20)(arraySlice(ts, 6), arraySlice(v, 6)) AS after_20,
    toString(arraySlice(all_samples, 4)) = toString(arraySlice(after_20, 4));

WITH
    [1, 2, 11, 12, 13, 21, 22, 31, 32, 33, 41, 42, 51, 52]::Array(DateTime) AS ts,
    [1e308, 0, 1e308, 3, 4, 6, 7, 8, 2, 3, 4, 5, 6, 7]::Array(Float64) AS v
SELECT
    'overflow increase',
    timeSeriesIncreaseToGrid(10, 60, 10, 20)(ts, v) AS all_samples,
    timeSeriesIncreaseToGrid(10, 60, 10, 20)(arraySlice(ts, 6), arraySlice(v, 6)) AS after_20,
    toString(arraySlice(all_samples, 4)) = toString(arraySlice(after_20, 4));

WITH
    [1, 2, 11, 12, 13, 21, 22, 31, 32, 33, 41, 42, 51, 52]::Array(DateTime) AS ts,
    [1e308, 0, 1e308, 3, 4, 6, 7, 8, 2, 3, 4, 5, 6, 7]::Array(Float64) AS v
SELECT
    'overflow rate',
    timeSeriesRateToGrid(10, 60, 10, 20)(ts, v) AS all_samples,
    timeSeriesRateToGrid(10, 60, 10, 20)(arraySlice(ts, 6), arraySlice(v, 6)) AS after_20,
    toString(arraySlice(all_samples, 4)) = toString(arraySlice(after_20, 4));

-- Many buckets: the grid points shared by a grid and a grid starting 500 steps later are equal (0 mismatches).
WITH
    arrayMap(n -> toDateTime(n * 10 + 5), range(2000)) AS ts,
    arrayMap(n -> if(n % 50 = 49, 1e16 + n, (n % 7) * 0.1 + 0.05), range(2000)) AS v
SELECT
    arrayCount((a, b) -> ifNull(toString(a), 'N') != ifNull(toString(b), 'N'),
        arraySlice(timeSeriesIncreaseToGrid(100, 20000, 10, 100)(ts, v), 501),
        timeSeriesIncreaseToGrid(5100, 20000, 10, 100)(ts, v)) AS increase_mismatches,
    arrayCount((a, b) -> ifNull(toString(a), 'N') != ifNull(toString(b), 'N'),
        arraySlice(timeSeriesRateToGrid(100, 20000, 10, 100)(ts, v), 501),
        timeSeriesRateToGrid(5100, 20000, 10, 100)(ts, v)) AS rate_mismatches,
    arrayCount((a, b) -> ifNull(toString(a), 'N') != ifNull(toString(b), 'N'),
        arraySlice(timeSeriesDeltaToGrid(100, 20000, 10, 100)(ts, v), 501),
        timeSeriesDeltaToGrid(5100, 20000, 10, 100)(ts, v)) AS delta_mismatches;

-- The same with a fractional reset in every bucket.
WITH
    arrayMap(n -> toDateTime(n * 10 + 5), range(2000)) AS ts,
    arrayMap(n -> ((n * 7919) % 1000) / 10, range(2000)) AS v
SELECT
    arrayCount((a, b) -> ifNull(toString(a), 'N') != ifNull(toString(b), 'N'),
        arraySlice(timeSeriesIncreaseToGrid(100, 20000, 10, 100)(ts, v), 501),
        timeSeriesIncreaseToGrid(5100, 20000, 10, 100)(ts, v)) AS increase_mismatches,
    arrayCount((a, b) -> ifNull(toString(a), 'N') != ifNull(toString(b), 'N'),
        arraySlice(timeSeriesRateToGrid(100, 20000, 10, 100)(ts, v), 501),
        timeSeriesRateToGrid(5100, 20000, 10, 100)(ts, v)) AS rate_mismatches;
