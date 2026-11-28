-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- A group with a single string, like `(c)`, `()` or `(?:)`, only extends the strings before it,
-- so a regex with many such groups is still a list of values and matches the same series as the regex does.

SET enable_time_series_table = 1;

DROP TABLE IF EXISTS rx_single;
CREATE TABLE rx_single ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0;

INSERT INTO rx_single (metric_name, tags, samples) VALUES
    ('a_total', {'job':'api'}, [(1000, 1.)]),
    ('a_total', {'job':'api-prod'}, [(1000, 2.)]),
    ('a_total', {'job':'api-stg'}, [(1000, 3.)]),
    ('a_total', {'job':'web-prod'}, [(1000, 4.)]),
    ('a_total', {'instance':'x'}, [(1000, 7.)]),
    ('b_total', {'job':'api'}, [(1000, 5.)]),
    ('c_total', {'job':'api'}, [(1000, 6.)]);

SELECT '-- a group with a single string extends the strings before it';
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx_single, 'a_total{job=~"a(p)i"}', 1000) ORDER BY value);
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx_single, 'a_total{job=~"(api|web)(-prod)"}', 1000) ORDER BY value);
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx_single, 'a_total{job=~"(api|web)(?:-)(prod)"}', 1000) ORDER BY value);
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx_single, 'a_total{job=~"api()(-stg|)(?:)"}', 1000) ORDER BY value);
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx_single, 'a_total{job=~"()"}', 1000) ORDER BY value);
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx_single, 'a_total{job!~"(api)(-)((prod))"}', 1000) ORDER BY value);

-- The primary key checks run without parallel replicas: they are about the plan of this server, not of a remote replica.
SELECT '-- a metric name regex with such groups reads the tags table by its primary key';
SELECT count() FROM timeSeriesSelector(rx_single, '{__name__=~"(a|b)(_)(total)()"}', 0, 2000, 'timestamp_and_value') SETTINGS force_primary_key = 1, enable_parallel_replicas = 0;
SELECT count() FROM timeSeriesSelector(rx_single, '{__name__=~"(c)(?:_total)"}', 0, 2000, 'timestamp_and_value') SETTINGS force_primary_key = 1, enable_parallel_replicas = 0;

SELECT '-- many groups with a single string after a long list of long strings';
SELECT count() FROM timeSeriesSelector(rx_single, concat('{__name__=~"(a|b)_total', repeat('()', 2000), '"}'), 0, 2000, 'timestamp_and_value') SETTINGS force_primary_key = 1, enable_parallel_replicas = 0;
SELECT count() FROM timeSeriesSelector(rx_single, concat('{__name__=~"(a|b|', arrayStringConcat(arrayMap(i -> 'x' || toString(i), range(98)), '|'), ')_tot', repeat('(?:)', 1000), '(a)(l)"}'), 0, 2000, 'timestamp_and_value') SETTINGS force_primary_key = 1, enable_parallel_replicas = 0;
SELECT count() FROM timeSeriesSelector(rx_single, concat('{__name__=~"(', arrayStringConcat(arrayMap(i -> 'x' || toString(i), range(100)), '|'), ')', repeat('y', 2000), repeat('(?:b)', 1000), '"}'), 0, 2000, 'timestamp_and_value') SETTINGS force_primary_key = 1, enable_parallel_replicas = 0;

SELECT '-- a single string after more than 100 strings keeps the regex';
SELECT count() FROM timeSeriesSelector(rx_single, concat('{__name__=~"(a|b|', arrayStringConcat(arrayMap(i -> 'x' || toString(i), range(150)), '|'), ')(_total)"}'), 0, 2000, 'timestamp_and_value');
SELECT count() FROM timeSeriesSelector(rx_single, concat('{__name__=~"(a|b|', arrayStringConcat(arrayMap(i -> 'x' || toString(i), range(150)), '|'), ')(_total)"}'), 0, 2000, 'timestamp_and_value') SETTINGS force_primary_key = 1, enable_parallel_replicas = 0; -- { serverError INDEX_NOT_USED }

DROP TABLE rx_single;
