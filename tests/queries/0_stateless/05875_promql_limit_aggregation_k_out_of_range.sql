-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- The argument `k` of the PromQL aggregation operators `topk`, `bottomk` and `limitk` is a `Float64` that is converted
-- to `UInt64`. A `k` that does not fit into `UInt64` is rejected, both when it is a constant and when it is calculated
-- by the query. The values here are the `Float64` numbers around 2^64, the first value that does not fit.
-- Only `topk` is used, because the three operators convert `k` in the same way.

DROP TABLE IF EXISTS prometheus;

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;

CREATE TABLE prometheus ENGINE = TimeSeries;

INSERT INTO prometheus (metric_name, tags, samples) VALUES ('m', map('host', 'h1'), [(toDateTime64(130, 3), 1)]);

-- 18446744073709549568 is 2^64 - 2048, the largest `Float64` below 2^64. It fits into `UInt64`, so it is accepted.
SELECT '-- largest k';
SELECT * FROM prometheusQuery('prometheus', 'topk(18446744073709549568, m)', 130);

-- 18446744073709551615 is 2^64 - 1, the largest `UInt64`. No `Float64` has this value: the number is parsed as the
-- nearest `Float64`, which is 2^64, so it is rejected.
SELECT '-- largest UInt64';
SELECT * FROM prometheusQuery('prometheus', 'topk(18446744073709551615, m)', 130); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

-- 18446744073709551616 is 2^64, the smallest `Float64` above the largest `UInt64`. It does not fit, so it is rejected.
SELECT '-- k is 2^64';
SELECT * FROM prometheusQuery('prometheus', 'topk(18446744073709551616, m)', 130); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

-- 18446744073709555712 is 2^64 + 4096, the smallest `Float64` above 2^64. It does not fit, so it is rejected.
SELECT '-- k above 2^64';
SELECT * FROM prometheusQuery('prometheus', 'topk(18446744073709555712, m)', 130); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

-- `scalar(count(m))` is 1 and is not a constant, so `k` is converted when the query runs.
-- `k` is 2^64 - 2048, the largest `Float64` below 2^64. It is accepted like the same constant.
SELECT '-- largest calculated k';
SELECT * FROM prometheusQuery('prometheus', 'topk(scalar(count(m)) * 18446744073709549568, m)', 130);

DROP TABLE prometheus;
