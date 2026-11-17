-- On windows not longer than the step, timeSeriesQuantileToGrid and timeSeriesMadToGrid select the order statistics from
-- unsorted values. The results must be the same as on overlapping windows, whose values are kept sorted.

SET enable_time_series_aggregate_functions = 1;

DROP TABLE IF EXISTS ts;
CREATE TABLE ts (timestamp DateTime, value Float64) ENGINE = MergeTree ORDER BY timestamp;
INSERT INTO ts SELECT number + 1, multiIf(number % 401 = 3, nan, number % 233 = 5, inf, number % 307 = 7, -inf, ((number * 7919) % 101 - 50) / 4) FROM numbers(3000);

-- The `*_sorted` grids have a window of 300 (301) seconds ending at every second, the others a window equal to the step.
-- Each array lists the grid points where the two differ.
SELECT
    arrayFilter(j -> toString(q[j]) != toString(q_sorted[300 * j - 299]), range(1, length(q) + 1)),
    arrayFilter(j -> toString(q_odd[j]) != toString(q_odd_sorted[301 * j - 300]), range(1, length(q_odd) + 1)),
    arrayFilter(j -> toString(mad[j]) != toString(mad_sorted[300 * j - 299]), range(1, length(mad) + 1)),
    arrayFilter(j -> toString(mad_odd[j]) != toString(mad_odd_sorted[301 * j - 300]), range(1, length(mad_odd) + 1)),
    arrayFilter(j -> toString(mad_f32[j]) != toString(mad_f32_sorted[300 * j - 299]), range(1, length(mad_f32) + 1))
FROM
(
    SELECT
        timeSeriesQuantileToGrid(300, 3000, 1, 300)(timestamp, value, 0.9) AS q_sorted,
        timeSeriesQuantileToGrid(300, 3000, 300, 300)(timestamp, value, 0.9) AS q,
        timeSeriesQuantileToGrid(301, 3000, 1, 301)(timestamp, value, 0.5) AS q_odd_sorted,
        timeSeriesQuantileToGrid(301, 3000, 301, 301)(timestamp, value, 0.5) AS q_odd,
        timeSeriesMadToGrid(300, 3000, 1, 300)(timestamp, value) AS mad_sorted,
        timeSeriesMadToGrid(300, 3000, 300, 300)(timestamp, value) AS mad,
        timeSeriesMadToGrid(301, 3000, 1, 301)(timestamp, value) AS mad_odd_sorted,
        timeSeriesMadToGrid(301, 3000, 301, 301)(timestamp, value) AS mad_odd,
        timeSeriesMadToGrid(300, 3000, 1, 300)(timestamp, value::Float32) AS mad_f32_sorted,
        timeSeriesMadToGrid(300, 3000, 300, 300)(timestamp, value::Float32) AS mad_f32
    FROM ts
);

-- The MAD of zeros is +0 whatever their signs, like `math.Abs` in Prometheus.
WITH [101, 102, 103]::Array(DateTime) AS timestamps, [0., -0., -0.]::Array(Float64) AS values
SELECT toString(timeSeriesMadToGrid(103, 103, 0, 10)(timestamps, values)), toString(timeSeriesMadToGrid(102, 103, 1, 10)(timestamps, values));

DROP TABLE ts;
