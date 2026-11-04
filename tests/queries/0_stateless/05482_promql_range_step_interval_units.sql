-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- The `step` of prometheusQueryRange() may be written as an INTERVAL literal; the
-- FunctionDocumentation of the table function (src/TableFunctions/TableFunctionTimeSeries.cpp)
-- spells that form out in its example, `... now() - INTERVAL 10 MINUTES, now(), INTERVAL 1 MINUTE`.
-- Turning such an interval into a PromQL duration is getFromIntervalKind() in
-- src/Parsers/Prometheus/parseTimeSeriesTypes.cpp: it multiplies the number of units by the length
-- of the unit in seconds and rescales the product to the scale of the result timestamps, which the
-- same documentation defines as "the scale of the timestamps in the table, but not less than 3
-- (milliseconds)". A year is exactly 365 days there, as in Prometheus, with no leap year.
--
-- No stateless test passed an INTERVAL as the step before, so none of that conversion ran. Each
-- unit is checked through the evaluation grid, which the documentation describes as iterating
-- "from start_time to end_time (inclusively)": over the range [start, start + 2 * step] the grid
-- holds exactly three points, so a duration that came out too large or too small changes the
-- count, and the distance between two neighbouring points is the decoded duration in seconds.
--
-- Scope note: the two rescaling branches for a result scale above 9 and below 3 are not reachable
-- from SQL, because that scale is max(scale of the timestamp column, 3) and DateTime64 stops at 9.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS promql_step_milli;
DROP TABLE IF EXISTS promql_step_nano;

CREATE TABLE promql_step_milli (samples Array(Tuple(DateTime64(3, 'UTC'), Float64))) ENGINE = TimeSeries;
CREATE TABLE promql_step_nano (samples Array(Tuple(DateTime64(9, 'UTC'), Float64))) ENGINE = TimeSeries;

SELECT '-- DateTime64(3) timestamps, result scale 3: unit, grid points, step in seconds';
SELECT 'nanosecond', length(samples), samples[2].1 - samples[1].1
    FROM prometheusQueryRange(promql_step_milli, 'time()', 1000, 1001, INTERVAL 500000000 NANOSECOND);
SELECT 'microsecond', length(samples), samples[2].1 - samples[1].1
    FROM prometheusQueryRange(promql_step_milli, 'time()', 1000, 1001, INTERVAL 500000 MICROSECOND);
SELECT 'millisecond', length(samples), samples[2].1 - samples[1].1
    FROM prometheusQueryRange(promql_step_milli, 'time()', 1000, 1001, INTERVAL 500 MILLISECOND);
SELECT 'second', length(samples), samples[2].1 - samples[1].1
    FROM prometheusQueryRange(promql_step_milli, 'time()', 1000, 1002, INTERVAL 1 SECOND);
SELECT 'minute', length(samples), samples[2].1 - samples[1].1
    FROM prometheusQueryRange(promql_step_milli, 'time()', 1000, 1120, INTERVAL 1 MINUTE);
SELECT 'hour', length(samples), samples[2].1 - samples[1].1
    FROM prometheusQueryRange(promql_step_milli, 'time()', 1000, 8200, INTERVAL 1 HOUR);
SELECT 'day', length(samples), samples[2].1 - samples[1].1
    FROM prometheusQueryRange(promql_step_milli, 'time()', 1000, 173800, INTERVAL 1 DAY);
SELECT 'week', length(samples), samples[2].1 - samples[1].1
    FROM prometheusQueryRange(promql_step_milli, 'time()', 1000, 1210600, INTERVAL 1 WEEK);
SELECT 'year', length(samples), samples[2].1 - samples[1].1
    FROM prometheusQueryRange(promql_step_milli, 'time()', 1000, 63073000, INTERVAL 1 YEAR);

-- A timestamp column of scale 9 rescales the sub-second units upwards instead of downwards. That
-- the result scale follows the column is covered by 05234_promql_result_timestamp_scale.
SELECT '-- DateTime64(9) timestamps, result scale 9: unit, grid points, step in seconds';
SELECT 'microsecond', length(samples), samples[2].1 - samples[1].1
    FROM prometheusQueryRange(promql_step_nano, 'time()', 1000, 1001, INTERVAL 500000 MICROSECOND);
SELECT 'millisecond', length(samples), samples[2].1 - samples[1].1
    FROM prometheusQueryRange(promql_step_nano, 'time()', 1000, 1001, INTERVAL 500 MILLISECOND);

-- A PromQL duration has no calendar units, so the units whose length is not a fixed number of
-- seconds are rejected instead of being approximated.
SELECT length(samples)
    FROM prometheusQueryRange(promql_step_milli, 'time()', 1000, 1001, INTERVAL 1 MONTH); -- { serverError BAD_ARGUMENTS }
SELECT length(samples)
    FROM prometheusQueryRange(promql_step_milli, 'time()', 1000, 1001, INTERVAL 1 QUARTER); -- { serverError BAD_ARGUMENTS }

-- 10^12 years in seconds does not fit Int64 at any scale.
SELECT length(samples)
    FROM prometheusQueryRange(promql_step_milli, 'time()', 1000, 1001, INTERVAL 1000000000000 YEAR); -- { serverError BAD_ARGUMENTS }

-- 10^10 seconds fits at scale 3 (as 10^13 milliseconds) but not at scale 9: the duration is held
-- in the ticks of the result scale, so the same INTERVAL is accepted by one table, not the other.
-- This is also what tells the two sections above apart in the results.
SELECT 'wide step at scale 3', length(samples)
    FROM prometheusQueryRange(promql_step_milli, 'time()', 1000, 1001, INTERVAL 10000000000 SECOND);
SELECT length(samples)
    FROM prometheusQueryRange(promql_step_nano, 'time()', 1000, 1001, INTERVAL 10000000000 SECOND); -- { serverError BAD_ARGUMENTS }

DROP TABLE promql_step_milli;
DROP TABLE promql_step_nano;
