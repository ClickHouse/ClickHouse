-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- Functions and operators drop the metric name `__name__`. If their vector argument selects one metric name
-- (`m`, `{__name__="m"}`, with offset or @ or as a subquery), its series can't get the same tags without the name,
-- so the converter skips the GROUP BY with the duplicate series check. The results must stay the same,
-- and selectors over several metric names must still merge or reject the series which get the same tags.

SET enable_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

INSERT INTO ts (metric_name, tags, samples) VALUES
    ('m', map('host', 'h1'), [(toDateTime64(100, 3), 1.0), (toDateTime64(110, 3), 2.0), (toDateTime64(120, 3), 4.0), (toDateTime64(130, 3), 8.0)]),
    ('m', map('host', 'h2'), [(toDateTime64(100, 3), -10.0), (toDateTime64(110, 3), 20.0), (toDateTime64(120, 3), 40.0), (toDateTime64(130, 3), 80.0)]),
    ('n', map('host', 'h1'), [(toDateTime64(100, 3), 5.0), (toDateTime64(110, 3), 15.0), (toDateTime64(120, 3), 25.0), (toDateTime64(130, 3), 35.0)]),
    ('old', map('host', 'h3'), [(toDateTime64(100, 3), 1.0), (toDateTime64(110, 3), 2.0)]),
    ('new', map('host', 'h3'), [(toDateTime64(120, 3), 3.0), (toDateTime64(130, 3), 4.0)]),
    ('p', map('host', 'h4'), [(toDateTime64(100, 3), 1.0)]),
    ('q', map('host', 'h4'), [(toDateTime64(110, 3), 2.0)]),
    ('r', map('host', 'h4'), [(toDateTime64(120, 3), 3.0), (toDateTime64(130, 3), 4.0)]),
    ('s', map('host', 'h4'), [(toDateTime64(130, 3), 5.0)]),
    ('b_bucket', map('le', '1'), [(toDateTime64(100, 3), 1.0), (toDateTime64(130, 3), 2.0)]),
    ('b_bucket', map('le', '+Inf'), [(toDateTime64(100, 3), 4.0), (toDateTime64(130, 3), 4.0)]);

SELECT '-- one metric name: every form';
SELECT * FROM prometheusQueryRange(ts, 'rate(m[20s])', 100, 130, 10) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'rate({__name__="m"}[20s])', 100, 130, 10) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'rate(m[20s] offset 10s)', 110, 130, 10) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'rate(m[20s] @ 130)', 100, 130, 10) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'max_over_time(m[20s:10s])', 100, 130, 10) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'abs(m)', 100, 130, 10) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'clamp_max(m, 5)', 100, 130, 10) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, '-m', 100, 130, 10) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'm * 2', 100, 130, 10) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'timestamp(m)', 100, 130, 10) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'histogram_quantile(0.5, b_bucket)', 100, 130, 10) ORDER BY ALL;
SELECT * FROM prometheusQuery(ts, 'sum(rate(m[20s]))', 130) ORDER BY ALL;

SELECT '-- several metric names: series with the same tags at different steps are merged';
SELECT * FROM prometheusQueryRange(ts, 'max_over_time({host="h3"}[5s])', 100, 130, 10) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'max_over_time({__name__=~"old|new"}[5s])', 100, 130, 10) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'max_over_time({__name__=~"p|q|r"}[5s])', 100, 130, 10) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'sum(max_over_time({__name__=~"p|q|r"}[5s]))', 100, 130, 10) ORDER BY ALL;

SELECT '-- several metric names: series with the same tags at the same step are rejected';
SELECT * FROM prometheusQueryRange(ts, 'rate({host="h1"}[20s])', 100, 130, 10); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQueryRange(ts, 'abs({host="h1"})', 100, 130, 10); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQueryRange(ts, 'abs({__name__=~"m|n", host="h1"})', 100, 130, 10); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQueryRange(ts, 'max_over_time({__name__=~"p|q|r|s"}[5s])', 100, 130, 10); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQueryRange(ts, 'sum(rate({host="h1"}[20s]))', 100, 130, 10); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

SELECT '-- one metric name: one aggregation step less than the same selector written as a regular expression';
SELECT
    (SELECT countIf(explain LIKE '%Aggregating%') FROM (EXPLAIN SELECT * FROM prometheusQueryRange(ts, 'rate(m[20s])', 100, 130, 10)))
    - (SELECT countIf(explain LIKE '%Aggregating%') FROM (EXPLAIN SELECT * FROM prometheusQueryRange(ts, 'rate({__name__=~"m|no_such_metric"}[20s])', 100, 130, 10)));
SELECT
    (SELECT countIf(explain LIKE '%Aggregating%') FROM (EXPLAIN SELECT * FROM prometheusQueryRange(ts, 'abs(m)', 100, 130, 10)))
    - (SELECT countIf(explain LIKE '%Aggregating%') FROM (EXPLAIN SELECT * FROM prometheusQueryRange(ts, 'abs({__name__=~"m|no_such_metric"})', 100, 130, 10)));

DROP TABLE ts;
