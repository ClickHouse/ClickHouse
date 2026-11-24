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
SELECT count() FROM timeSeriesSelector(rx, '{__name__=~"(a|b)_total"}', 0, 2000, 'timestamp_and_value') SETTINGS force_primary_key = 1;
SELECT count() FROM timeSeriesSelector(rx, '{__name__=~"(a|b|c)_(total)"}', 0, 2000, 'timestamp_and_value') SETTINGS force_primary_key = 1;

SELECT '-- too many combinations of groups keep the regex';
SELECT count() FROM timeSeriesSelector(rx, '{__name__=~"(a|b|c|d|e|f|g|h|i|j|k)(_|)(total|v|w|x|y|z|q|r|s|t)"}', 0, 2000, 'timestamp_and_value');
SELECT count() FROM timeSeriesSelector(rx, '{__name__=~"(a|b|c|d|e|f|g|h|i|j|k)(_|)(total|v|w|x|y|z|q|r|s|t)"}', 0, 2000, 'timestamp_and_value') SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }

SELECT '-- a big group with literals around it keeps the regex, a big group alone is a list';
SELECT count() FROM timeSeriesSelector(rx, concat('{__name__=~"(a|b|', arrayStringConcat(arrayMap(i -> 'x' || toString(i), range(150)), '|'), ')_total"}'), 0, 2000, 'timestamp_and_value');
SELECT count() FROM timeSeriesSelector(rx, concat('{__name__=~"(a|b|', arrayStringConcat(arrayMap(i -> 'x' || toString(i), range(150)), '|'), ')_total"}'), 0, 2000, 'timestamp_and_value') SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }
SELECT count() FROM timeSeriesSelector(rx, concat('{__name__=~"a_(', arrayStringConcat(arrayMap(i -> 'x' || toString(i), range(150)), '|'), '|total)"}'), 0, 2000, 'timestamp_and_value') SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }
SELECT count() FROM timeSeriesSelector(rx, concat('{__name__=~"(a_total|b_total|', arrayStringConcat(arrayMap(i -> 'x' || toString(i), range(150)), '|'), ')"}'), 0, 2000, 'timestamp_and_value') SETTINGS force_primary_key = 1;

SELECT '-- groups nested deeper than 32 keep the regex';
SELECT count() FROM timeSeriesSelector(rx, concat('{__name__=~"', repeat('(', 32), 'a_total|b_total', repeat(')', 32), '"}'), 0, 2000, 'timestamp_and_value') SETTINGS force_primary_key = 1;
SELECT count() FROM timeSeriesSelector(rx, concat('{__name__=~"', repeat('(', 33), 'a_total|b_total', repeat(')', 33), '"}'), 0, 2000, 'timestamp_and_value');
SELECT count() FROM timeSeriesSelector(rx, concat('{__name__=~"', repeat('(', 33), 'a_total|b_total', repeat(')', 33), '"}'), 0, 2000, 'timestamp_and_value') SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx, concat('a_total{job=~"', repeat('(', 40), 'api-prod|api-stg', repeat(')', 40), '"}'), 1000) ORDER BY value);

DROP TABLE rx;

-- A missing tag is stored as NULL in a Nullable dedicated column, and a list with an empty string must match it as an empty value.
DROP TABLE IF EXISTS rx_nullable;
DROP TABLE IF EXISTS rx_lc_nullable;
CREATE TABLE rx_nullable ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0, tags_to_columns = {'job': 'job'}
TAGS INNER COLUMNS (job Nullable(String));
CREATE TABLE rx_lc_nullable ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0, tags_to_columns = {'job': 'job'}
TAGS INNER COLUMNS (job LowCardinality(Nullable(String)));

INSERT INTO rx_nullable (metric_name, tags, samples) VALUES
    ('a_total', {'job':'api'}, [(1000, 1.)]),
    ('a_total', {'job':'web'}, [(1000, 2.)]),
    ('a_total', {'instance':'x'}, [(1000, 3.)]);
INSERT INTO rx_lc_nullable (metric_name, tags, samples) VALUES
    ('a_total', {'job':'api'}, [(1000, 1.)]),
    ('a_total', {'job':'web'}, [(1000, 2.)]),
    ('a_total', {'instance':'x'}, [(1000, 3.)]);

SELECT '-- a missing tag in a Nullable dedicated column is an empty value';
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx_nullable, 'a_total{job!~"(api|)()"}', 1000) ORDER BY value) SETTINGS transform_null_in = 1;
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx_nullable, 'a_total{job!~"(api|)()"}', 1000) ORDER BY value) SETTINGS transform_null_in = 0;
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx_nullable, 'a_total{job=~"(api|)()"}', 1000) ORDER BY value);
SELECT '-- a missing tag in a LowCardinality(Nullable) dedicated column is an empty value';
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx_lc_nullable, 'a_total{job!~"(api|)()"}', 1000) ORDER BY value) SETTINGS transform_null_in = 1;
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx_lc_nullable, 'a_total{job!~"(api|)()"}', 1000) ORDER BY value) SETTINGS transform_null_in = 0;
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx_lc_nullable, 'a_total{job=~"(api|)()"}', 1000) ORDER BY value);

DROP TABLE rx_nullable;
DROP TABLE rx_lc_nullable;

-- A dedicated column without NULL is compared as is, so a skipping index on it serves a list.
DROP TABLE IF EXISTS rx_indexed;
CREATE TABLE rx_indexed ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0, tags_index_granularity = 1, tags_to_columns = {'job': 'job'}
TAGS INNER COLUMNS (job String, INDEX job_idx job TYPE bloom_filter GRANULARITY 1);

INSERT INTO rx_indexed (metric_name, tags, samples) VALUES
    ('a_total', {'job':'api'}, [(1000, 1.)]),
    ('a_total', {'job':'web'}, [(1000, 2.)]),
    ('a_total', {'job':'db'}, [(1000, 3.)]),
    ('a_total', {'instance':'x'}, [(1000, 4.)]);

SELECT '-- a list on an indexed dedicated column without NULL uses the skipping index';
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx_indexed, 'a_total{job=~"api|web"}', 1000) ORDER BY value)
SETTINGS log_comment = '05453_rx_indexed', use_skip_indexes_on_data_read = 0, use_query_condition_cache = 0, enable_parallel_replicas = 0;
SYSTEM FLUSH LOGS query_log;
SELECT arrayExists(index -> endsWith(index, '.job_idx'), used_data_skipping_indices) FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment = '05453_rx_indexed';

DROP TABLE rx_indexed;
