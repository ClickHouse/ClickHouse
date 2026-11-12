-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- PromQL info(): text of a user matcher that looks like a placeholder is matched literally.

SET enable_time_series_table = 1;
SET session_timezone = 'UTC';

CREATE TABLE ts ENGINE = TimeSeries;

INSERT INTO ts (metric_name, tags, samples) VALUES
    ('metric', map('instance', 'a', 'job', '1'), [(toDateTime64(0, 3), 1)]),
    ('target_info', map('instance', 'a', 'job', '1', 'data', '@@instance@@'), [(toDateTime64(0, 3), 1)]),
    ('target_info', map('instance', 'a', 'job', '1', 'data', 'a'), [(toDateTime64(0, 3), 1)]);

SELECT * FROM prometheusQuery(ts, 'info(metric, {data="@@instance@@"})', 0) ORDER BY ALL;
SELECT * FROM prometheusQuery(ts, 'info(metric, {data="@@job@@"})', 0) ORDER BY ALL;
SELECT * FROM prometheusQuery(ts, 'info(metric, {data=~"@@.*"})', 0) ORDER BY ALL;

DROP TABLE ts;
