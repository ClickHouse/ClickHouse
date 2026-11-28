-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- Each `label_replace` / `label_join` over an instant vector adds one subquery level.

SET enable_time_series_table = 1;
SET session_timezone = 'UTC';
SET max_subquery_depth = 13;

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('m', map('k', 'a'), [(toDateTime64(100, 3), 1)]),
    ('m', map('k', 'b'), [(toDateTime64(200, 3), 2)]),
    ('n', map('k', 'a'), [(toDateTime64(100, 3), 3)]),
    ('n', map('k', 'b'), [(toDateTime64(100, 3), 4)]);

SELECT '-- 5 nested label_replace';
SELECT tags, value FROM prometheusQuery(ts, concat(repeat('label_replace(', 5), 'm', repeat(', "d", "$1", "k", "(.*)")', 5)), 200) ORDER BY tags;
SELECT '-- 5 nested label_join';
SELECT tags, value FROM prometheusQuery(ts, concat(repeat('label_join(', 5), 'm', repeat(', "d", "-", "k")', 5)), 200) ORDER BY tags;

SET prefer_column_name_to_alias = 1;

SELECT '-- prefer_column_name_to_alias: series which get the same tags are merged';
SELECT * FROM prometheusQueryRange(ts, 'label_replace(last_over_time(m[50s]), "k", "z", "k", ".*")', 100, 200, 100) ORDER BY ALL;
SELECT '-- prefer_column_name_to_alias: series with values at the same step are still reported';
SELECT * FROM prometheusQueryRange(ts, 'label_join(last_over_time(n[50s]), "k", "-", "nx")', 100, 200, 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT '-- prefer_column_name_to_alias: 5 nested label_replace';
SELECT tags, value FROM prometheusQuery(ts, concat(repeat('label_replace(', 5), 'm', repeat(', "d", "$1", "k", "(.*)")', 5)), 200) ORDER BY tags;

DROP TABLE ts;
