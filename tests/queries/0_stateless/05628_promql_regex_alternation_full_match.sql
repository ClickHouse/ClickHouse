-- Tags: no-fasttest, no-tsan
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-tsan: an `info` query needs more stack than the 5% that TSan allows.

-- A PromQL regex matcher matches the whole label value, also when the regex has alternatives.

SET enable_time_series_table = 1;
SET session_timezone = 'UTC';

CREATE TABLE ts ENGINE = TimeSeries;
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('m', map('version', '1.0'), [(toDateTime64(0, 3), 1)]),
    ('m', map('version', '2.0'), [(toDateTime64(0, 3), 2)]),
    ('m', map('version', '1.0-extra'), [(toDateTime64(0, 3), 3)]),
    ('m', map('version', 'x2.0'), [(toDateTime64(0, 3), 4)]),
    ('metric', map('instance', 'a', 'job', '1'), [(toDateTime64(0, 3), 5)]),
    ('target_info', map('instance', 'a', 'job', '1', 'version', '1.0-extra'), [(toDateTime64(0, 3), 1)]),
    ('n', map('data', 'a$'), [(toDateTime64(0, 3), 6)]),
    ('n', map('data', 'a$extra'), [(toDateTime64(0, 3), 7)]),
    ('n', map('data', 'foo|bar'), [(toDateTime64(0, 3), 8)]),
    ('n', map('data', 'foo'), [(toDateTime64(0, 3), 9)]),
    ('metric2', map('instance', 'b', 'job', '1'), [(toDateTime64(0, 3), 10)]),
    ('target_info', map('instance', 'b', 'job', '1', 'data', 'a$extra'), [(toDateTime64(0, 3), 1)]),
    ('metric3', map('instance', 'c', 'job', '1'), [(toDateTime64(0, 3), 11)]),
    ('target_info', map('instance', 'c', 'job', '1', 'data', 'foo|bar'), [(toDateTime64(0, 3), 1)]);

SELECT '-- =~ in a selector';
SELECT tags, value FROM prometheusQuery(ts, 'm{version=~"1\\\\.0|2\\\\.0"}', 0) ORDER BY ALL;
SELECT '-- !~ in a selector';
SELECT tags, value FROM prometheusQuery(ts, 'm{version!~"1\\\\.0|2\\\\.0"}', 0) ORDER BY ALL;
SELECT '-- =~ in the label selector of info';
SELECT tags, value FROM prometheusQuery(ts, 'info(metric, {version=~"1\\\\.0|2\\\\.0"})', 0) ORDER BY ALL;
SELECT '-- !~ in the label selector of info';
SELECT tags, value FROM prometheusQuery(ts, 'info(metric, {version!~"1\\\\.0|2\\\\.0"})', 0) ORDER BY ALL;

-- An escaped `$` is not an anchor, and `\Q` quotes up to the end of the regex.
SELECT '-- escaped $ in a selector';
SELECT tags, value FROM prometheusQuery(ts, 'n{data=~"a\\\\$"}', 0) ORDER BY ALL;
SELECT tags, value FROM prometheusQuery(ts, 'n{data!~"a\\\\$"}', 0) ORDER BY ALL;
SELECT '-- quoted regex in a selector';
SELECT tags, value FROM prometheusQuery(ts, 'n{data=~"\\\\Qfoo|bar"}', 0) ORDER BY ALL;
SELECT tags, value FROM prometheusQuery(ts, 'n{data!~"\\\\Qfoo|bar"}', 0) ORDER BY ALL;
SELECT '-- escaped $ in the label selector of info';
SELECT tags, value FROM prometheusQuery(ts, 'info(metric2, {data=~"a\\\\$"})', 0) ORDER BY ALL;
SELECT tags, value FROM prometheusQuery(ts, 'info(metric2, {data!~"a\\\\$"})', 0) ORDER BY ALL;
SELECT '-- quoted regex in the label selector of info';
SELECT tags, value FROM prometheusQuery(ts, 'info(metric3, {data=~"\\\\Qfoo|bar"})', 0) ORDER BY ALL;
SELECT tags, value FROM prometheusQuery(ts, 'info(metric3, {data!~"\\\\Qfoo|bar"})', 0) ORDER BY ALL;
SELECT tags, value FROM prometheusQuery(ts, 'info(metric3, {__name__=~"\\\\Qtarget_info"})', 0) ORDER BY ALL;

DROP TABLE ts;
