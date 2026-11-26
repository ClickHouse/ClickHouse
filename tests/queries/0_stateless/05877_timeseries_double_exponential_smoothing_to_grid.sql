-- Tests timeSeriesDoubleExponentialSmoothingToGrid: the forms of its arguments, merging of partial states, windows with
-- fewer than two samples, and the checks of the smoothing factor and the trend factor. The PromQL function built on it
-- is tested in 05876_promql_double_exponential_smoothing.

SELECT '-- disabled by default';
-- The function is in private preview.
SET enable_time_series_aggregate_functions = 0;
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45, 0.5, 0.5)(toDateTime(110), 1::Float64); -- { serverError UNKNOWN_AGGREGATE_FUNCTION }

SET enable_time_series_aggregate_functions = 1;

-- The grid is [90, 105, ..., 210] with a window of 45 seconds. The windows of the grid points 120, 135, 150 and 165
-- hold {1, 1}, {1, 1, 3}, {1, 1, 3, 4} and {3, 4}, the window of 210 holds {5, 5, 8}, and the others hold at most one
-- sample.
-- `grp` splits the samples into two partial states for the merge test below.
CREATE TABLE smoothing_input (grp UInt8, timestamp DateTime, value Float64) ENGINE = MergeTree ORDER BY timestamp;
INSERT INTO smoothing_input VALUES (0, 110, 1), (1, 120, 1), (0, 130, 3), (1, 140, 4), (0, 190, 5), (1, 200, 5), (0, 210, 8);

SELECT '-- three argument forms';
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45, 0.5, 0.5)(timestamp, value)
FROM smoothing_input;

SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45, 0.5, 0.5)(timestamps, values)
FROM (SELECT groupArray(timestamp) AS timestamps, groupArray(value) AS values FROM smoothing_input);

SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45, 0.5, 0.5)(samples)
FROM (SELECT groupArray((timestamp, value)) AS samples FROM smoothing_input);

SELECT '-- other factors';
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45, 0.25, 0.75)(timestamp, value)
FROM smoothing_input;

SELECT '-- Float32 values';
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45, 0.5, 0.5)(timestamp, value::Float32)
FROM smoothing_input;

SELECT '-- factors as Decimal and String';
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45, 0.5::Decimal(9, 2), '0.5')(timestamp, value)
FROM smoothing_input;

SELECT '-- partial states merge';
SELECT timeSeriesDoubleExponentialSmoothingToGridMerge(90, 210, 15, 45, 0.5, 0.5)(st)
FROM (SELECT timeSeriesDoubleExponentialSmoothingToGridState(90, 210, 15, 45, 0.5, 0.5)(timestamp, value) AS st FROM smoothing_input GROUP BY grp);

SELECT '-- window not a multiple of the step';
-- Every step is then split into two buckets. The window of the grid point 150 loses the sample at 110 and holds {1, 3, 4}.
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 40, 0.5, 0.5)(timestamp, value)
FROM smoothing_input;

SELECT '-- window shorter than the step';
-- The grid is [90, 120, 150, 180, 210] and the windows hold {1, 1}, {4} and {5, 8} at 120, 150 and 210.
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 30, 20, 0.5, 0.5)(timestamp, value)
FROM smoothing_input;

SELECT '-- unsorted and duplicate timestamps';
-- The samples are smoothed in the order of their timestamps, and of two values with the same timestamp the larger one is used.
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45, 0.5, 0.5)([200, 130, 110, 210, 140, 130, 120, 190]::Array(DateTime), [5, 2, 1, 8, 4, 3, 1, 5]::Array(Float64));

SELECT '-- fewer than two samples';
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45, 0.5, 0.5)(timestamp, value)
FROM smoothing_input WHERE timestamp = 110;

SELECT '-- factor out of range';
-- The smoothing factor must be greater than 0.
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45, 0, 0.5)(timestamp, value)
FROM smoothing_input; -- { serverError BAD_ARGUMENTS }

-- The trend factor must be less than 1.
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45, 0.5, 2)(timestamp, value)
FROM smoothing_input; -- { serverError BAD_ARGUMENTS }

-- Like in Prometheus, the factors are checked before the number of samples, so one sample is enough for the error.
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45, 2, 2)(timestamp, value)
FROM smoothing_input WHERE timestamp = 110; -- { serverError BAD_ARGUMENTS }

SELECT '-- factor not checked without samples';
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45, 2, 2)(timestamp, value)
FROM smoothing_input WHERE 0;

SELECT '-- NaN factor is accepted';
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45, nan, 0.5)(timestamp, value)
FROM smoothing_input;

SELECT '-- invalid parameters and arguments';
-- Both factors are missing.
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45)(timestamp, value)
FROM smoothing_input; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- The smoothing factor is not a number.
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45, 'abc', 0.5)(timestamp, value)
FROM smoothing_input; -- { serverError BAD_ARGUMENTS }

-- The trend factor is NULL.
SELECT timeSeriesDoubleExponentialSmoothingToGrid(90, 210, 15, 45, 0.5, NULL)(timestamp, value)
FROM smoothing_input; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE smoothing_input;
