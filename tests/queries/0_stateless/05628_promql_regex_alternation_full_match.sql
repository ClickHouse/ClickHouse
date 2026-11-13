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
    ('target_info', map('instance', 'a', 'job', '1', 'version', '1.0-extra'), [(toDateTime64(0, 3), 1)]);

SELECT '-- =~ in a selector';
SELECT tags, value FROM prometheusQuery(ts, 'm{version=~"1\\\\.0|2\\\\.0"}', 0) ORDER BY ALL;
SELECT '-- !~ in a selector';
SELECT tags, value FROM prometheusQuery(ts, 'm{version!~"1\\\\.0|2\\\\.0"}', 0) ORDER BY ALL;
SELECT '-- =~ in the label selector of info';
SELECT tags, value FROM prometheusQuery(ts, 'info(metric, {version=~"1\\\\.0|2\\\\.0"})', 0) ORDER BY ALL;
SELECT '-- !~ in the label selector of info';
SELECT tags, value FROM prometheusQuery(ts, 'info(metric, {version!~"1\\\\.0|2\\\\.0"})', 0) ORDER BY ALL;

DROP TABLE ts;
