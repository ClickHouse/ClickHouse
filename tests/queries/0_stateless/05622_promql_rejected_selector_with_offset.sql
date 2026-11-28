-- Tags: no-fasttest

SET enable_time_series_table = 1;

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

-- The bare selector is rejected with a parse error.
SELECT * FROM prometheusQuery('ts', '{job=~".*"}', 1000); -- { serverError CANNOT_PARSE_PROMQL_QUERY }

-- A selector without a non-empty matcher, followed by `offset` or `@`.
SELECT * FROM prometheusQuery('ts', '{job=~".*"} offset 5m', 1000); -- { serverError CANNOT_PARSE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('ts', '{job=~".*"} @ 100', 1000); -- { serverError CANNOT_PARSE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('ts', 'rate({job!="a"}[5m] offset 1m)', 1000); -- { serverError CANNOT_PARSE_PROMQL_QUERY }

-- An invalid regular expression, followed by `offset`.
SELECT * FROM prometheusQuery('ts', 'up{job=~"("} offset 5m', 1000); -- { serverError CANNOT_PARSE_PROMQL_QUERY }

-- The metric name set twice, and an invalid escape sequence, followed by `offset`.
SELECT * FROM prometheusQuery('ts', 'up{__name__="x"} offset 5m', 1000); -- { serverError CANNOT_PARSE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('ts', 'up{job="\\q"} offset 5m', 1000); -- { serverError CANNOT_PARSE_PROMQL_QUERY }

-- A valid selector with `offset`: the metric name is a non-empty matcher. The table is empty, so there are no rows.
SELECT * FROM prometheusQuery('ts', 'up{job=~".*"} offset 5m', 1000);

DROP TABLE ts;
