-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- A regex of literals with groups before, after or between them is a list of values,
-- so it is matched with `IN`, and a metric name regex like that reads the tags table by its primary key.

SET enable_time_series_table = 1;

DROP TABLE IF EXISTS rx;
CREATE TABLE rx ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0;

INSERT INTO rx (metric_name, tags, samples) VALUES
    ('a_total', {'job':'api'}, [(1000, 1.)]),
    ('a_total', {'job':'api-prod'}, [(1000, 2.)]),
    ('a_total', {'job':'api-stg'}, [(1000, 3.)]),
    ('a_total', {'job':'web-prod'}, [(1000, 4.)]),
    ('a_total', {'instance':'x'}, [(1000, 5.)]),
    ('b_total', {'job':'api'}, [(1000, 6.)]),
    ('c_total', {'job':'api'}, [(1000, 7.)]);

SELECT '-- a group is expanded with the literals around it';
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx, 'a_total{job=~"api-(prod|stg)"}', 1000) ORDER BY value);
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx, 'a_total{job=~"(api|web)-prod"}', 1000) ORDER BY value);
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx, 'a_total{job=~"(api|web)(-prod|)"}', 1000) ORDER BY value);
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx, 'a_total{job=~"(|api-)stg"}', 1000) ORDER BY value);
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx, 'a_total{job!~"api(-(prod|stg)|)"}', 1000) ORDER BY value);

SELECT '-- a quantifier after a group keeps the regex';
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx, 'a_total{job=~"api(-prod)?"}', 1000) ORDER BY value);

SELECT '-- a metric name regex with groups reads the tags table by its primary key';
SELECT count() FROM timeSeriesSelector(rx, '{__name__=~"(a|b)_total"}', 0, 2000) SETTINGS force_primary_key = 1;
SELECT count() FROM timeSeriesSelector(rx, '{__name__=~"(a|b|c)_(total)"}', 0, 2000) SETTINGS force_primary_key = 1;

SELECT '-- too many combinations of groups keep the regex';
SELECT count() FROM timeSeriesSelector(rx, '{__name__=~"(a|b|c|d|e|f|g|h|i|j|k)(_|)(total|v|w|x|y|z|q|r|s|t)"}', 0, 2000);
SELECT count() FROM timeSeriesSelector(rx, '{__name__=~"(a|b|c|d|e|f|g|h|i|j|k)(_|)(total|v|w|x|y|z|q|r|s|t)"}', 0, 2000) SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }

DROP TABLE rx;
