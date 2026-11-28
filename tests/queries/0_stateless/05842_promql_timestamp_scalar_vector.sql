-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- The PromQL function `timestamp` applied to an instant vector that is NOT a selector, i.e. the
-- scalar-valued branches of applyTimestampToGeneralExpression in
-- src/Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionTimestamp.cpp.
-- Expected behaviour per the comment above that function and Prometheus's own `timestamp()`:
-- such an expression is materialized at every query step, so each present sample gets the
-- evaluation time of its step (in seconds since the epoch) as its value.
-- `vector(1)` is such an argument: an instant vector without labels carrying a constant scalar.
-- A selector argument takes a different branch and is covered by 05241_prometheus_query_timestamp.

SET enable_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

INSERT INTO ts (metric_name, tags, samples) VALUES ('m', map('job', 'a'), [(toDateTime64(100, 3), 1.0)]);

SELECT 'range query: one series without labels, every step carries its own timestamp as the value';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples)
FROM prometheusQueryRange(ts, 'timestamp(vector(1))', 100, 200, 50);

SELECT 'a sub-second step keeps the fractional seconds';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Milli(x.1), x.2), samples)
FROM prometheusQueryRange(ts, 'timestamp(vector(1))', 100, 200.5, 50.25);

SELECT 'instant query: a single evaluation time';
SELECT tags, toUnixTimestamp64Milli(timestamp), value FROM prometheusQuery(ts, 'timestamp(vector(1))', 200);

SELECT 'an argument that is empty by construction gives an empty result';
SELECT count() FROM prometheusQuery(ts, 'timestamp(clamp(m, 1, -1))', 200);
SELECT count() FROM prometheusQueryRange(ts, 'timestamp(clamp(m, 1, -1))', 100, 200, 50);

-- A subquery aligns its inner evaluation times to multiples of its step. At 200 the window of
-- [1s:1m] is (199, 200], which holds no multiple of 1m, so the inner `timestamp` is never
-- evaluated and the result is empty; [1m:30s] holds 150 and 180, so the last one wins.
SELECT 'an empty inner evaluation range gives an empty result';
SELECT count() FROM prometheusQuery(ts, 'last_over_time(timestamp(vector(1))[1s:1m])', 200);
SELECT 'a non-empty one returns the last inner evaluation time';
SELECT tags, toUnixTimestamp64Milli(timestamp), value
FROM prometheusQuery(ts, 'last_over_time(timestamp(vector(1))[1m:30s])', 200);

DROP TABLE ts;
