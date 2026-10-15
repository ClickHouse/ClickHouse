-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- absent() and timestamp() of a range function over a scalar subquery must see the windows without a sample.
-- The subquery is evaluated at 999, 1002, ..., 1020: the 1500ms window at 1010 has no sample, and the 3500ms windows
-- at 1000, 1010 and 1015 have one sample, too few for predict_linear.

DROP TABLE IF EXISTS ts;
SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

SELECT '-- absent';
SELECT * FROM prometheusQueryRange(ts, 'absent(last_over_time(vector(1)[1500ms:3s]))', 1000, 1020, 5) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'absent(quantile_over_time(0.5, vector(1)[1500ms:3s]))', 1000, 1020, 5) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'absent(predict_linear(vector(1)[3500ms:3s], 10))', 1000, 1020, 5) ORDER BY ALL;

SELECT '-- timestamp';
SELECT * FROM prometheusQueryRange(ts, 'timestamp(last_over_time(vector(1)[1500ms:3s]))', 1000, 1020, 5) ORDER BY ALL;

DROP TABLE ts;
