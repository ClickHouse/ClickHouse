-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- Argument validation and the degenerate inputs of the PromQL functions `absent` and `absent_over_time`
-- (applyFunctionAbsent in src/Storages/TimeSeries/PrometheusQueryToSQL/). Both follow Prometheus's
-- absent()/absent_over_time(): exactly one argument, an instant vector for `absent` and a range vector for
-- `absent_over_time`, and the result is a synthetic series with the value 1 only where the input has no sample.
-- Valid calls over real selectors are covered by 05211_promql_present_over_time_absent_over_time.

SET enable_time_series_table = 1;
SET enable_time_series_aggregate_functions = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

INSERT INTO ts (metric_name, tags, samples) VALUES
    ('up', map('instance', 'h1'), [(toDateTime64(1700000000, 3), 1.0)]);

SELECT '-- both functions expect exactly one argument';
SELECT * FROM prometheusQuery(ts, 'absent(up, up)', 1700000000); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(ts, 'absent_over_time(up[5m], up[5m])', 1700000000); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

SELECT '-- absent expects an instant vector, absent_over_time expects a range vector';
SELECT * FROM prometheusQuery(ts, 'absent(up[5m])', 1700000000); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(ts, 'absent(1)', 1700000000); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(ts, 'absent_over_time(up)', 1700000000); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

-- In a subquery `<expression>[<range>:<step>]` the inner expression is evaluated on the range
-- [<range_start> - <range> + <step>, <range_end>], both ends rounded down to a multiple of <step>. A range
-- shorter than the step leaves that interval empty, so the `absent` node has no step to evaluate at all and
-- contributes no series.
SELECT '-- an empty evaluation range produces no rows';
SELECT tags, value FROM prometheusQuery(ts, 'last_over_time(absent(up)[1s:1m])', 1700000000);
SELECT tags, value FROM prometheusQuery(ts, 'last_over_time(absent_over_time(up[5m])[1s:1m])', 1700000000);

-- A scalar turned into an instant vector has a value at every evaluation step, so it is never absent.
SELECT '-- absent of vector(<scalar>) is empty';
SELECT tags, value FROM prometheusQuery(ts, 'absent(vector(1))', 1700000000);

-- The inner `absent` is known to be empty without reading anything, so the outer one reports the whole range as
-- absent. Its argument is not a selector, so the synthetic series carries no tags, like Prometheus's
-- createLabelsForAbsentFunction().
SELECT '-- absent of an always-empty instant vector is the synthetic series with the value 1';
SELECT tags, value FROM prometheusQuery(ts, 'absent(absent(vector(1)))', 1700000000);

DROP TABLE ts;
