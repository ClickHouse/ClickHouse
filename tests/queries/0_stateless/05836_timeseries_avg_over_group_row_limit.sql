-- Tags: long, no-sanitizers
-- Tag no-sanitizers: adds 50 million rows to aggregation states, too slow with sanitizers.
-- Tag long: for the flaky check.

-- A state cannot get more rows than a reader accepts, neither by adding rows nor by merging states.
SET enable_time_series_aggregate_functions = 1;

SELECT ignore(timeSeriesAvgOverGroupState(tuple(), CAST([], 'Array(Float64)'))) FROM numbers(16777215) SETTINGS max_threads = 1;
SELECT timeSeriesAvgOverGroup(tuple(), CAST([], 'Array(Float64)')) FROM numbers(16777216) SETTINGS max_threads = 1; -- { serverError TOO_LARGE_ARRAY_SIZE }
SELECT timeSeriesAvgOverGroupMerge(s) FROM
(
    SELECT timeSeriesAvgOverGroupState(tuple(), CAST([], 'Array(Float64)')) AS s FROM numbers(8388607)
    UNION ALL
    SELECT timeSeriesAvgOverGroupState(tuple(), CAST([], 'Array(Float64)')) FROM numbers(8388609)
) SETTINGS max_threads = 1; -- { serverError TOO_LARGE_ARRAY_SIZE }
