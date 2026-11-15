-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

SET enable_time_series_table = 1;

DROP TABLE IF EXISTS rx;
CREATE TABLE rx ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0;

INSERT INTO rx (metric_name, tags, samples) VALUES
    ('m', {'pod':'gold-1'}, [(1000, 1.)]),
    ('m', {'pod':'app-gold-1'}, [(1000, 2.)]),
    ('m', {'pod':'zzz-1'}, [(1000, 3.)]),
    ('m', {'pod':'zzz'}, [(1000, 4.)]);

SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx, 'm{pod=~"zzz|gold.*"}', 1000) ORDER BY value);
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx, 'm{pod=~"gold.*|zzz"}', 1000) ORDER BY value);
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx, 'm{pod=~"^zzz|gold.*$"}', 1000) ORDER BY value);
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx, 'm{pod!~"zzz|gold.*"}', 1000) ORDER BY value);

DROP TABLE rx;
