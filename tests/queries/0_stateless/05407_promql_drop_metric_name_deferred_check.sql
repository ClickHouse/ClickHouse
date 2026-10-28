-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- Functions like `rate` drop the metric name. Series which differ only by the metric name are duplicates if they have values
-- at the same step, otherwise they are merged into one series. The check runs only for the series which get the same tags,
-- and an aggregation over series with one metric name needs no check at all.

SET enable_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

-- The steps are 100, 200 and 300, every range below is [20s], so each step sees only the two samples 10 seconds apart
-- right before it, and `rate` of them is 1.
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('a', map('host', '1'), [(toDateTime64(90, 3), 10), (toDateTime64(100, 3), 20), (toDateTime64(190, 3), 10), (toDateTime64(200, 3), 20), (toDateTime64(290, 3), 10), (toDateTime64(300, 3), 20)]),
    ('b', map('host', '1'), [(toDateTime64(90, 3), 10), (toDateTime64(100, 3), 20)]),
    ('c', map('host', '2'), [(toDateTime64(190, 3), 10), (toDateTime64(200, 3), 20)]),
    ('d', map('host', '2'), [(toDateTime64(90, 3), 10), (toDateTime64(100, 3), 20)]),
    ('e', map('host', '3'), [(toDateTime64(90, 3), 10), (toDateTime64(100, 3), 20), (toDateTime64(190, 3), 30), (toDateTime64(200, 3), 40)]),
    ('f', map('host', '2'), [(toDateTime64(290, 3), 10), (toDateTime64(300, 3), 20)]),
    ('g', map('host', '2'), [(toDateTime64(190, 3), 10), (toDateTime64(200, 3), 20)]);

SELECT '-- series with values at the same step are duplicates after the metric name is dropped';
SELECT * FROM prometheusQueryRange(ts, 'rate({__name__=~"a|b", host="1"}[20s])', 100, 300, 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQueryRange(ts, 'sum(rate({__name__=~"a|b", host="1"}[20s]))', 100, 300, 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQueryRange(ts, 'sum by (host) (rate({__name__=~"a|b", host="1"}[20s]))', 100, 300, 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(ts, 'rate({__name__=~"a|b", host="1"}[20s])', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQueryRange(ts, 'rate({__name__=~"c|d|f|g", host="2"}[20s])', 100, 300, 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

SELECT '-- series with values at different steps are merged after the metric name is dropped';
SELECT * FROM prometheusQueryRange(ts, 'rate({__name__=~"c|d", host="2"}[20s])', 100, 300, 100) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'rate({__name__=~"c|d|f", host="2"}[20s])', 100, 300, 100) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'sum(rate({__name__=~"c|d", host="2"}[20s]))', 100, 300, 100) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'sum by (host) (rate({__name__=~"c|d|e"}[20s]))', 100, 300, 100) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'topk(1, rate({__name__=~"c|d", host="2"}[20s]))', 100, 300, 100) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'last_over_time(rate({__name__=~"c|d", host="2"}[20s])[100s:100s])', 100, 300, 100) ORDER BY ALL;

SELECT '-- series of one metric stay as they are';
SELECT * FROM prometheusQueryRange(ts, 'rate(e[20s])', 100, 300, 100) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'sum(rate(e[20s]))', 100, 300, 100) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'sum by (host) (rate(e[20s]))', 100, 300, 100) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'sum by (__name__) (rate(e[20s]))', 100, 300, 100) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'sum without (host) (rate(e[20s]))', 100, 300, 100) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'abs(rate(e[20s])) * 2', 100, 300, 100) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'rate(e[20s]) > 0.5', 100, 300, 100) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'topk(1, rate(e[20s]))', 100, 300, 100) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'label_replace(rate(e[20s]), "x", "y", "", "")', 100, 300, 100) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'last_over_time(rate(e[20s])[100s:100s])', 100, 300, 100) ORDER BY ALL;
SELECT * FROM prometheusQuery(ts, 'sum(rate(e[20s]))', 100) ORDER BY ALL;

DROP TABLE ts;
