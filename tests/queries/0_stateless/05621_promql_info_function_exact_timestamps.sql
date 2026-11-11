-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

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
SELECT '-- two info series with the same nanosecond timestamp are still a conflict';
INSERT INTO ts_ns (metric_name, tags, samples) VALUES
    ('target_info', map('instance', 'a', 'job', '1', 'data', 'third'), [(toDateTime64('2026-11-11 00:00:00.000000002', 9, 'UTC'), 1)]);
SELECT * FROM prometheusQuery(ts_ns, 'info(metric)', toDateTime64('2026-11-11 00:00:01', 9, 'UTC')); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
DROP TABLE ts_ns;
