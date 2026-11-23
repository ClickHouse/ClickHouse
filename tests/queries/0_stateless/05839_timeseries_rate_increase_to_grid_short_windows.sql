-- On windows not longer than the step, nothing of the previous window may leak into the next one in `timeSeriesRateToGrid`
-- and `timeSeriesIncreaseToGrid`: an infinite counter must not turn it into NaN, and a large reset must not cost precision.
-- The second window must give the same result as computed alone.

SET enable_time_series_aggregate_functions = 1;

WITH
    [90, 100, 190, 200]::Array(DateTime) AS timestamps,
    [-inf, inf, 1., 2.] AS values,
    timeSeriesRateToGrid(100, 200, 100, 30)(timestamps, values) AS rate,
    timeSeriesIncreaseToGrid(100, 200, 100, 30)(timestamps, values) AS increase
SELECT
    'infinite counter',
    arrayMap(x -> round(x, 9), rate),
    arrayMap(x -> round(x, 9), increase),
    rate[2] = timeSeriesRateToGrid(200, 200, 0, 30)(timestamps, values)[1],
    increase[2] = timeSeriesIncreaseToGrid(200, 200, 0, 30)(timestamps, values)[1];

WITH
    [90, 100, 180, 190, 200]::Array(DateTime) AS timestamps,
    [1e15, 1., 1.3, 0.1, 0.2] AS values,
    timeSeriesRateToGrid(100, 200, 100, 30)(timestamps, values) AS rate,
    timeSeriesIncreaseToGrid(100, 200, 100, 30)(timestamps, values) AS increase
SELECT
    'large reset',
    arrayMap(x -> round(x, 9), rate),
    arrayMap(x -> round(x, 9), increase),
    rate[2] = timeSeriesRateToGrid(200, 200, 0, 30)(timestamps, values)[1],
    increase[2] = timeSeriesIncreaseToGrid(200, 200, 0, 30)(timestamps, values)[1];
