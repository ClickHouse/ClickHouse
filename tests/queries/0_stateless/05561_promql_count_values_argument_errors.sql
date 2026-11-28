-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- Argument validation of the PromQL aggregation operator count_values.
-- Per docs/en/interfaces/prometheus.md the operator is count_values(<label>, <instant-vector>):
-- exactly two arguments, a string label name and an instant vector.

DROP TABLE IF EXISTS prometheus;

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;

CREATE TABLE prometheus ENGINE = TimeSeries;

INSERT INTO prometheus (metric_name, tags, samples) VALUES
    ('m', map('host', 'h1'), [(toDateTime64(100, 3), 1.5), (toDateTime64(110, 3), 2.5)]),
    ('m', map('host', 'h2'), [(toDateTime64(110, 3), 2.5)]),
    ('m', map('host', 'h3'), [(toDateTime64(110, 3), 7)]);

SELECT '-- count_values expects exactly 2 arguments';
SELECT * FROM prometheusQuery('prometheus', 'count_values(m)', 110); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('prometheus', 'count_values("v")', 110); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('prometheus', 'count_values("v", m, m)', 110); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

SELECT '-- the first argument must be a string';
SELECT * FROM prometheusQuery('prometheus', 'count_values(1, m)', 110); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('prometheus', 'count_values(m, m)', 110); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

SELECT '-- the second argument must be an instant vector';
SELECT * FROM prometheusQuery('prometheus', 'count_values("v", 1)', 110); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('prometheus', 'count_values("v", "x")', 110); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

SELECT '-- a valid call counts the series per distinct sample value';
SELECT * FROM prometheusQuery('prometheus', 'count_values("v", m)', 110) ORDER BY tags;

DROP TABLE prometheus;
