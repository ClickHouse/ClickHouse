-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- Tests the PromQL function `double_exponential_smoothing(v, sf, tf)`: for each series it smooths the samples of the
-- range `v` with the smoothing factor `sf` and the trend factor `tf`, and returns the last smoothed value.
-- The data and the expected values of the first two scenarios are the ones of the Prometheus test file
-- `promqltest/testdata/functions.test`. The other scenarios check the cases without a result and the rejected arguments.

SET session_timezone = 'UTC';
SET enable_time_series_table = 1;

DROP TABLE IF EXISTS ts_positive;
DROP TABLE IF EXISTS ts_negative;
DROP TABLE IF EXISTS ts_range;

CREATE TABLE ts_positive ENGINE = TimeSeries;
CREATE TABLE ts_negative ENGINE = TimeSeries;
CREATE TABLE ts_range ENGINE = TimeSeries;

-- The data of "positive trends" in the Prometheus test file: one sample every 10 seconds. Each row here is one piece
-- of a series, for example `100+30x1000` is 1001 samples starting with 100 and growing by 30. `first_index` is the
-- position of the first sample of the piece in the series, so the piece starts at `first_index * 10` seconds.
-- `http_requests_mix` and `http_requests_histogram` are left out: they hold native histograms, which the `TimeSeries`
-- table engine does not store.
INSERT INTO ts_positive (metric_name, tags, samples)
SELECT 'http_requests', tags, arrayMap(i -> (toDateTime64((first_index + i) * 10, 3), toFloat64(first_value + i * step)), range(num_samples))
FROM VALUES('tags Map(String, String), first_index UInt32, first_value UInt32, step UInt32, num_samples UInt32',
    (map('job', 'api-server', 'instance', '0', 'group', 'production'), 0, 0, 10, 1001),
    (map('job', 'api-server', 'instance', '0', 'group', 'production'), 1001, 100, 30, 1001),
    (map('job', 'api-server', 'instance', '1', 'group', 'production'), 0, 0, 20, 1001),
    (map('job', 'api-server', 'instance', '1', 'group', 'production'), 1001, 200, 30, 1001),
    (map('job', 'api-server', 'instance', '0', 'group', 'canary'), 0, 0, 30, 1001),
    (map('job', 'api-server', 'instance', '0', 'group', 'canary'), 1001, 300, 80, 1001),
    (map('job', 'api-server', 'instance', '1', 'group', 'canary'), 0, 0, 40, 2001));

-- The data of "negative trends" in the Prometheus test file, in the same form. A negative `step` is a falling piece.
INSERT INTO ts_negative (metric_name, tags, samples)
SELECT 'http_requests', tags, arrayMap(i -> (toDateTime64((first_index + i) * 10, 3), toFloat64(first_value + i * step)), range(num_samples))
FROM VALUES('tags Map(String, String), first_index UInt32, first_value UInt32, step Int32, num_samples UInt32',
    (map('job', 'api-server', 'instance', '0', 'group', 'production'), 0, 8000, -10, 1001),
    (map('job', 'api-server', 'instance', '1', 'group', 'production'), 0, 0, -20, 1001),
    (map('job', 'api-server', 'instance', '0', 'group', 'canary'), 0, 0, 30, 1001),
    (map('job', 'api-server', 'instance', '0', 'group', 'canary'), 1001, 300, -80, 1001),
    (map('job', 'api-server', 'instance', '1', 'group', 'canary'), 0, 0, -40, 1001),
    (map('job', 'api-server', 'instance', '1', 'group', 'canary'), 1001, 0, 40, 1001));

-- A series that is not a straight line and has a gap between 140 and 190 seconds.
INSERT INTO ts_range (metric_name, tags, samples)
VALUES ('m', map('host', 'h1'),
    [(toDateTime64(110, 3), 1),
    (toDateTime64(120, 3), 1),
    (toDateTime64(130, 3), 3),
    (toDateTime64(140, 3), 4),
    (toDateTime64(190, 3), 5),
    (toDateTime64(200, 3), 5),
    (toDateTime64(210, 3), 8)]);

-- Every series is a straight line inside the window, so the result is its last sample: 8000, 16000, 24000 and 32000.
SELECT '-- positive trends';
SELECT * FROM prometheusQuery('ts_positive', 'double_exponential_smoothing(http_requests[1m], 0.01, 0.1)', 8000) ORDER BY ALL;

-- Again every series is a straight line inside the window, so the result is its last sample, the one at 8000 seconds.
-- It is sample number 800 of its piece: 8000 - 10 * 800 = 0, -20 * 800 = -16000, 30 * 800 = 24000 and -40 * 800 = -32000.
SELECT '-- negative trends';
SELECT * FROM prometheusQuery('ts_negative', 'double_exponential_smoothing(http_requests[1m], 0.01, 0.1)', 8000) ORDER BY ALL;

-- The function needs at least two samples of the same series. The window (-60, 0] holds one sample of each of the
-- four series, the one at 0 seconds, so no series has a result.
SELECT '-- fewer than two samples';
SELECT * FROM prometheusQuery('ts_positive', 'double_exponential_smoothing(http_requests[1m], 0.01, 0.1)', 0) ORDER BY ALL;

-- Each step smooths the samples of its own window again, starting from the first sample of that window.
-- The steps are 90, 105, ..., 210 and each window is 45 seconds long. The steps 90 and 105 have no samples, 180 and 195
-- have one sample, so they have no result. The windows of the other steps hold the samples {1, 1}, {1, 1, 3},
-- {1, 1, 3, 4}, {3, 4} and {5, 5, 8}, which give 1, 2, 3.25, 4 and 6.5.
SELECT '-- range query';
SELECT * FROM prometheusQueryRange('ts_range', 'double_exponential_smoothing(m[45s], 0.5, 0.5)', 90, 210, 15) ORDER BY ALL;

-- The range can be a subquery. `max_over_time(m[15s])` is evaluated every 15 seconds and gives 1, 3 and 4 at 120, 135
-- and 150 seconds, nothing at 165 and 180 seconds, then 5 and 8 at 195 and 210 seconds. The function sees only these
-- values, not the samples of `m`: the steps 135, 150, 165 and 210 get {1, 3}, {1, 3, 4}, {3, 4} and {5, 8}, which give
-- 3, 4.5, 4 and 8. The other steps get fewer than two values.
SELECT '-- subquery';
SELECT * FROM prometheusQueryRange('ts_range', 'double_exponential_smoothing(max_over_time(m[15s])[45s:15s], 0.5, 0.5)', 90, 210, 15) ORDER BY ALL;

SELECT '-- wrong number of arguments';
SELECT * FROM prometheusQuery('ts_positive', 'double_exponential_smoothing(http_requests[1m])', 8000); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('ts_positive', 'double_exponential_smoothing(http_requests[1m], 0.01)', 8000); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('ts_positive', 'double_exponential_smoothing(http_requests[1m], 0.01, 0.1, 0.1)', 8000); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

-- The first argument must be a range vector, and both factors must be scalars.
SELECT '-- wrong argument types';
SELECT * FROM prometheusQuery('ts_positive', 'double_exponential_smoothing(http_requests, 0.01, 0.1)', 8000); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('ts_positive', 'double_exponential_smoothing(http_requests[1m], "0.01", 0.1)', 8000); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('ts_positive', 'double_exponential_smoothing(http_requests[1m], 0.01, http_requests)', 8000); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

-- Both factors must be greater than 0 and less than 1, like in Prometheus.
SELECT '-- factor out of range';
SELECT * FROM prometheusQuery('ts_positive', 'double_exponential_smoothing(http_requests[1m], 0, 0.1)', 8000); -- { serverError BAD_ARGUMENTS }
SELECT * FROM prometheusQuery('ts_positive', 'double_exponential_smoothing(http_requests[1m], 1, 0.1)', 8000); -- { serverError BAD_ARGUMENTS }
SELECT * FROM prometheusQuery('ts_positive', 'double_exponential_smoothing(http_requests[1m], -0.5, 0.1)', 8000); -- { serverError BAD_ARGUMENTS }
SELECT * FROM prometheusQuery('ts_positive', 'double_exponential_smoothing(http_requests[1m], 0.01, 0)', 8000); -- { serverError BAD_ARGUMENTS }
SELECT * FROM prometheusQuery('ts_positive', 'double_exponential_smoothing(http_requests[1m], 0.01, 1)', 8000); -- { serverError BAD_ARGUMENTS }
SELECT * FROM prometheusQuery('ts_positive', 'double_exponential_smoothing(http_requests[1m], 0.01, 1.5)', 8000); -- { serverError BAD_ARGUMENTS }
-- One sample is enough for the check: the window (-60, 0] holds one sample of each series.
SELECT * FROM prometheusQuery('ts_positive', 'double_exponential_smoothing(http_requests[1m], 2, 2)', 0); -- { serverError BAD_ARGUMENTS }

-- Like in Prometheus, the factors are checked only for a window that holds samples. The window (99940, 100000] is empty.
SELECT '-- factor not checked without samples';
SELECT * FROM prometheusQuery('ts_positive', 'double_exponential_smoothing(http_requests[1m], 2, 2)', 100000) ORDER BY ALL;

-- `scalar(count(http_requests))` is calculated when the query runs. Only constant factors are supported.
SELECT '-- calculated factor';
SELECT * FROM prometheusQuery('ts_positive', 'double_exponential_smoothing(http_requests[1m], scalar(count(http_requests)) / 8, 0.1)', 8000); -- { serverError NOT_IMPLEMENTED }

DROP TABLE ts_positive;
DROP TABLE ts_negative;
DROP TABLE ts_range;
