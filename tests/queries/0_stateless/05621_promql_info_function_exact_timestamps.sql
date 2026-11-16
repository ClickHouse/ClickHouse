-- Tags: no-fasttest, no-tsan, long
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag long: the flaky check runs a changed test in many parallel copies, which makes the many `info` queries exceed 180 seconds.
-- Tag no-tsan: an `info` query needs more stack than the 5% that TSan allows.

-- PromQL info(): the info series are compared by their exact timestamps.

SET enable_time_series_table = 1;
SET session_timezone = 'UTC';

SELECT '-- the newest info series is chosen by the exact timestamp of a DateTime64(9) table';
CREATE TABLE ts_ns (samples Array(Tuple(DateTime64(9, 'UTC'), Float64))) ENGINE = TimeSeries;
INSERT INTO ts_ns (metric_name, tags, samples) VALUES
    ('metric', map('instance', 'a', 'job', '1'), [(toDateTime64('2026-11-11 00:00:00.000000002', 9, 'UTC'), 1)]),
    ('target_info', map('instance', 'a', 'job', '1', 'data', 'first'), [(toDateTime64('2026-11-11 00:00:00.000000001', 9, 'UTC'), 1)]),
    ('target_info', map('instance', 'a', 'job', '1', 'data', 'second'), [(toDateTime64('2026-11-11 00:00:00.000000002', 9, 'UTC'), 1)]);
SELECT tags, value FROM prometheusQuery(ts_ns, 'info(metric)', toDateTime64('2026-11-11 00:00:01', 9, 'UTC')) ORDER BY tags;
SELECT '-- the same over a range longer than 2^53 nanoseconds';
SELECT tags, samples FROM prometheusQueryRange(ts_ns, 'info(metric)', toDateTime64('2026-07-29 00:00:01', 9, 'UTC'), toDateTime64('2026-11-11 00:00:01', 9, 'UTC'), 9072000) ORDER BY tags;
SELECT '-- two info series with the same nanosecond timestamp are still a conflict';
INSERT INTO ts_ns (metric_name, tags, samples) VALUES
    ('target_info', map('instance', 'a', 'job', '1', 'data', 'third'), [(toDateTime64('2026-11-11 00:00:00.000000002', 9, 'UTC'), 1)]);
SELECT * FROM prometheusQuery(ts_ns, 'info(metric)', toDateTime64('2026-11-11 00:00:01', 9, 'UTC')); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT * FROM prometheusQueryRange(ts_ns, 'info(metric)', toDateTime64('2026-07-29 00:00:01', 9, 'UTC'), toDateTime64('2026-11-11 00:00:01', 9, 'UTC'), 9072000); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
DROP TABLE ts_ns;

SELECT '-- a table with UInt32 timestamps is read when the lookback starts before 1970';
CREATE TABLE ts_u32 (samples Array(Tuple(UInt32, Float64))) ENGINE = TimeSeries;
CREATE TABLE ts_u32_v10 (samples Array(Tuple(UInt32, Float64))) ENGINE = TimeSeries SETTINGS version = 10;
INSERT INTO ts_u32 (metric_name, tags, samples) VALUES
    ('metric', map('instance', 'a', 'job', '1'), [(0, 5)]),
    ('target_info', map('instance', 'a', 'job', '1', 'data', 'first'), [(0, 1)]),
    ('target_info', map('instance', 'a', 'job', '1', 'data', 'second'), [(1, 1)]);
INSERT INTO ts_u32_v10 (metric_name, tags, samples) VALUES
    ('metric', map('instance', 'a', 'job', '1'), [(0, 5)]),
    ('target_info', map('instance', 'a', 'job', '1', 'data', 'first'), [(0, 1)]),
    ('target_info', map('instance', 'a', 'job', '1', 'data', 'second'), [(1, 1)]);
SELECT tags, value FROM prometheusQuery(ts_u32, 'info(metric)', 1) ORDER BY tags;
SELECT tags, value FROM prometheusQuery(ts_u32_v10, 'info(metric)', 1) ORDER BY tags;
DROP TABLE ts_u32;
DROP TABLE ts_u32_v10;
