-- Tags: no-fasttest
-- The time series aggregate functions need an explicit setting, and the query is heavy enough
-- that the fuzzer would slow down on it.

SET enable_time_series_aggregate_functions = 1;

-- A flat series must give deriv exactly 0 and prediction exactly the constant: merging into the
-- empty accumulator of every grid point used to run the weighted Chan update, whose `c * n / n`
-- rounds for values like 0.1 and turned the flat deriv non-zero.
SELECT timeSeriesDerivToGrid(90, 150, 30, 60)(
    arrayMap(i -> toUInt32(i * 10), range(1, 31)),
    arrayMap(i -> 0.1, range(1, 31)));

SELECT timeSeriesPredictLinearToGrid(90, 150, 30, 60, 0)(
    arrayMap(i -> toUInt32(i * 10), range(1, 31)),
    arrayMap(i -> 0.1, range(1, 31)));

-- A window with a single populated bucket merges into the empty accumulator too (the recompute
-- path resets the accumulator per grid point), so instant predict is affected as well.
SELECT timeSeriesPredictLinearToGrid(90, 150, 30, 60, 0)(
    arrayMap(i -> toUInt32(i * 10), range(11, 14)),
    arrayMap(i -> 0.1, range(11, 14)));
