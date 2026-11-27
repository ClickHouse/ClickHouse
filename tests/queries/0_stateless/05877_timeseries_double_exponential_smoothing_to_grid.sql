-- Tests timeSeriesDoubleExponentialSmoothingToGrid: the forms of its arguments, merging of partial states, windows with
-- fewer than two samples, and the smoothing factor and the trend factor, which follow the samples as numbers or as
-- arrays with one value per grid point. The PromQL function built on it is tested in
-- 05876_promql_double_exponential_smoothing.

SELECT '-- disabled by default';
-- The function is in private preview.
SET enable_time_series_aggregate_functions = 0;
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45)(toDateTime(110), 1::Float64, 0.5, 0.5); -- { serverError UNKNOWN_AGGREGATE_FUNCTION }

SET enable_time_series_aggregate_functions = 1;

-- The grid is [90, 105, ..., 210] with a window of 45 seconds. The windows of the grid points 120, 135, 150 and 165
-- hold {1, 1}, {1, 1, 3}, {1, 1, 3, 4} and {3, 4}, the window of 210 holds {5, 5, 8}, and the others hold at most one
-- sample.
-- `grp` splits the samples into two partial states for the merge test below.
CREATE TABLE smoothing_input (grp UInt8, timestamp DateTime, value Float64) ENGINE = MergeTree ORDER BY timestamp;
INSERT INTO smoothing_input VALUES (0, 110, 1), (1, 120, 1), (0, 130, 3), (1, 140, 4), (0, 190, 5), (1, 200, 5), (0, 210, 8);

SELECT '-- three argument forms';
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45)(timestamp, value, 0.5, 0.5)
FROM smoothing_input;

SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45)(timestamps, values, 0.5, 0.5)
FROM (SELECT groupArray(timestamp) AS timestamps, groupArray(value) AS values FROM smoothing_input);

SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45)(samples, 0.5, 0.5)
FROM (SELECT groupArray((timestamp, value)) AS samples FROM smoothing_input);

SELECT '-- other factors';
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45)(timestamp, value, 0.25, 0.75)
FROM smoothing_input;

SELECT '-- Float32 values';
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45)(timestamp, value::Float32, 0.5, 0.5)
FROM smoothing_input;

SELECT '-- Float32 factors';
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45)(timestamp, value, 0.5::Float32, 0.5::Float32)
FROM smoothing_input;

SELECT '-- factors per grid point';
-- The smoothing factor is 0.45, 0.5, 0.55 and 0.7 at the grid points 135, 150, 165 and 210.
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45)(timestamp, value, [0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7], 0.5)
FROM smoothing_input;

-- The trend factor is 0.25 at the grid point 150, the only one whose window holds more than three samples.
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45)(timestamp, value, 0.5, [0.5, 0.5, 0.5, 0.5, 0.25, 0.5, 0.5, 0.5, 0.5])
FROM smoothing_input;

-- The windows of the grid points 90 and 105 are empty, so the factor 2 is not checked there.
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45)(timestamp, value, [2, 2, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5], 0.5)
FROM smoothing_input;

SELECT '-- partial states merge';
SELECT timeSeriesDoubleExponentialSmoothingToGridMerge(90, 210, 15, 45)(st)
FROM (SELECT timeSeriesDoubleExponentialSmoothingToGridState(90, 210, 15, 45)(timestamp, value, 0.5, 0.5) AS st FROM smoothing_input GROUP BY grp);

SELECT '-- window not a multiple of the step';
-- Every step is then split into two buckets. The window of the grid point 150 loses the sample at 110 and holds {1, 3, 4}.
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 40)(timestamp, value, 0.5, 0.5)
FROM smoothing_input;

SELECT '-- window shorter than the step';
-- The grid is [90, 120, 150, 180, 210] and the windows hold {1, 1}, {4} and {5, 8} at 120, 150 and 210.
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 30, 20)(timestamp, value, 0.5, 0.5)
FROM smoothing_input;

SELECT '-- unsorted and duplicate timestamps';
-- The samples are smoothed in the order of their timestamps, and of two values with the same timestamp the larger one is used.
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45)([200, 130, 110, 210, 140, 130, 120, 190]::Array(DateTime), [5, 2, 1, 8, 4, 3, 1, 5]::Array(Float64), 0.5, 0.5);

SELECT '-- fewer than two samples';
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45)(timestamp, value, 0.5, 0.5)
FROM smoothing_input WHERE timestamp = 110;

SELECT '-- factor out of range';
-- The smoothing factor must be greater than 0.
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45)(timestamp, value, 0, 0.5)
FROM smoothing_input; -- { serverError BAD_ARGUMENTS }

-- The trend factor must be less than 1.
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45)(timestamp, value, 0.5, 2)
FROM smoothing_input; -- { serverError BAD_ARGUMENTS }

-- Like in Prometheus, the factors are checked before the number of samples, so one sample is enough for the error.
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45)(timestamp, value, 2, 2)
FROM smoothing_input WHERE timestamp = 110; -- { serverError BAD_ARGUMENTS }

SELECT '-- factor not checked without samples';
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45)(timestamp, value, 2, 2)
FROM smoothing_input WHERE 0;

SELECT '-- NaN factor is accepted';
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45)(timestamp, value, nan, 0.5)
FROM smoothing_input;

SELECT '-- factors that differ';
-- The smoothing factor must be the same in every row.
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45)(timestamp, value, 0.5 - grp / 4, 0.5)
FROM smoothing_input; -- { serverError BAD_ARGUMENTS }

-- The trend factor must be the same in every merged state.
SELECT timeSeriesDoubleExponentialSmoothingToGridMerge(90, 210, 15, 45)(st)
FROM (SELECT timeSeriesDoubleExponentialSmoothingToGridState(90, 210, 15, 45)(timestamp, value, 0.5, 0.5 - grp / 4) AS st FROM smoothing_input GROUP BY grp); -- { serverError BAD_ARGUMENTS }

-- An array must have one value per grid point: nine, not two.
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45)(timestamp, value, [0.5, 0.5], 0.5)
FROM smoothing_input; -- { serverError BAD_ARGUMENTS }

SELECT '-- invalid parameters and arguments';
-- Both factors are missing.
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45)(timestamp, value)
FROM smoothing_input; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- The factors are arguments, not parameters.
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45, 0.5, 0.5)(timestamp, value)
FROM smoothing_input; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- The smoothing factor is a string.
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45)(timestamp, value, '0.5', 0.5)
FROM smoothing_input; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The trend factor is a `Decimal`, which is not a native number.
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45)(timestamp, value, 0.5, 0.5::Decimal(9, 2))
FROM smoothing_input; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE smoothing_input;
