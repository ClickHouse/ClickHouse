-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

SET enable_time_series_table = 1;

DROP TABLE IF EXISTS rx;
CREATE TABLE rx ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0;

INSERT INTO rx (metric_name, tags, samples) VALUES
    ('m', {'pod':'gold-1'}, [(1000, 1.)]),
    ('m', {'pod':'app-gold-1'}, [(1000, 2.)]),
    ('m', {'pod':'zzz-1'}, [(1000, 3.)]),
    ('m', {'pod':'zzz'}, [(1000, 4.)]),
    ('m', {'pod':'gold.*'}, [(1000, 5.)]),
    ('m', {'pod':'zzz|1'}, [(1000, 6.)]);

SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx, 'm{pod=~"zzz|gold.*"}', 1000) ORDER BY value);
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx, 'm{pod=~"gold.*|zzz"}', 1000) ORDER BY value);
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx, 'm{pod=~"^zzz|gold.*$"}', 1000) ORDER BY value);
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx, 'm{pod!~"zzz|gold.*"}', 1000) ORDER BY value);

SELECT '-- a quote without the end extends to the end of the regex';
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx, 'm{pod=~"zzz|\\\\Qgold.*"}', 1000) ORDER BY value);
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx, 'm{pod!~"zzz|\\\\Qgold.*"}', 1000) ORDER BY value);
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx, 'm{pod=~"\\\\Qgold.*"}', 1000) ORDER BY value);

SELECT '-- alternatives inside a group, a character class or a quote are not wrapped';
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx, 'm{pod=~"(zzz|gold)-1"}', 1000) ORDER BY value);
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx, 'm{pod=~"zz[|z]"}', 1000) ORDER BY value);
SELECT groupArray(value) FROM (SELECT value FROM prometheusQuery(rx, 'm{pod=~"\\\\Qzzz|\\\\E.*"}', 1000) ORDER BY value);

SELECT '-- generated regular expressions';

-- A regex without an alternation at the top level keeps its leading literal, so the primary key on `metric_name`
-- can filter by its fixed prefix (`extractFixedPrefixFromRegularExpression` stops at a leading group).
SELECT count() FROM timeSeriesSelector(rx, '{__name__=~"^(node_foo|node_bar)$"}', 0, 2000, 'timestamp_and_value') SETTINGS log_comment = '05770_1';
SELECT count() FROM timeSeriesSelector(rx, '{__name__=~"m.*"}', 0, 2000, 'timestamp_and_value') SETTINGS log_comment = '05770_2';
SELECT count() FROM timeSeriesSelector(rx, '{__name__=~"zzz|m.*"}', 0, 2000, 'timestamp_and_value') SETTINGS log_comment = '05770_3';
SELECT count() FROM timeSeriesSelector(rx, '{__name__=~"\\\\Qm"}', 0, 2000, 'timestamp_and_value') SETTINGS log_comment = '05770_4';
SELECT count() FROM timeSeriesSelector(rx, '{__name__=~"zzz|\\\\Qm"}', 0, 2000, 'timestamp_and_value') SETTINGS log_comment = '05770_5';

SYSTEM FLUSH LOGS query_log, text_log;
SELECT q.log_comment, extract(t.message, 'match\\(metric_name, (\'[^\']*\')\\)')
FROM system.text_log AS t
INNER JOIN
(
    SELECT query_id, log_comment FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND startsWith(log_comment, '05770_')
) AS q USING (query_id)
WHERE t.event_date >= yesterday() AND t.logger_name = 'StorageTimeSeriesSelector' AND startsWith(t.message, 'Will execute query:')
ORDER BY q.log_comment
SETTINGS max_rows_to_read = 0; -- system.text_log can be really big

DROP TABLE rx;
