-- Random settings limits: max_threads=(1, 1)
-- The sample buckets of `timeSeries*ToGrid` are allocated in the arena: window functions and `runningAccumulate` must provide one.
-- `runningAccumulate` restarts at every block, and more threads can split the sorted states into several blocks.

SET allow_experimental_time_series_aggregate_functions = 1;
SET allow_deprecated_error_prone_window_functions = 1;

SELECT id, t, timeSeriesRateToGrid(0, 200, 50, 100)(toDateTime(t, 'UTC'), v) OVER (PARTITION BY id ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
FROM (SELECT number % 2 AS id, intDiv(number, 2) * 10 AS t, (number * 3 % 7)::Float64 AS v FROM numbers(40))
ORDER BY id, t;

SELECT slice, runningAccumulate(state)
FROM
(
    SELECT intDiv(t, 50) AS slice, timeSeriesRateToGridState(0, 200, 50, 100)(toDateTime(t, 'UTC'), v) AS state
    FROM (SELECT intDiv(number, 2) * 10 AS t, (number * 3 % 7)::Float64 AS v FROM numbers(40) WHERE number % 2 = 0)
    GROUP BY slice
    ORDER BY slice
);
