-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas: the replicas execute the `timeSeriesTags` table function without coordinating
-- with the initiator, so a `count()` answered from the primary key index alone is summed up once per replica.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_prewarm, ts_prewarm_cols;

SELECT '--- a restarted table skips the time series already stored ---';
CREATE TABLE ts_prewarm ENGINE = TimeSeries TAGS INNER ENGINE = MergeTree ORDER BY (metric_name, id);
INSERT INTO ts_prewarm (metric_name, tags, samples) VALUES
    ('http_requests', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:00', 3), 1.)]),
    ('http_requests', {'job': 'web'}, [(toDateTime64('2026-01-01 00:00:01', 3), 2.)]);
DETACH TABLE ts_prewarm;
ATTACH TABLE ts_prewarm;
INSERT INTO ts_prewarm (metric_name, tags, samples) VALUES
    ('http_requests', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:02', 3), 3.)]),
    ('http_requests', {'job': 'web'}, [(toDateTime64('2026-01-01 00:00:03', 3), 4.)]);
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts_prewarm);

SELECT '--- a new tag set is still written ---';
INSERT INTO ts_prewarm (metric_name, tags, samples) VALUES ('http_requests', {'job': 'db'}, [(toDateTime64('2026-01-01 00:00:04', 3), 5.)]);
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts_prewarm);

SELECT '--- clearing the cache makes the next insert write the rows again ---';
SYSTEM CLEAR TIME SERIES CACHES ts_prewarm;
INSERT INTO ts_prewarm (metric_name, tags, samples) VALUES ('http_requests', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:05', 3), 6.)]);
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts_prewarm);

SELECT '--- the same check with a tag stored in its own column ---';
CREATE TABLE ts_prewarm_cols ENGINE = TimeSeries SETTINGS tags_to_columns = {'job': 'job'} TAGS INNER ENGINE = MergeTree ORDER BY (metric_name, id);
INSERT INTO ts_prewarm_cols (metric_name, tags, samples) VALUES
    ('http_requests', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:00', 3), 1.)]),
    ('http_requests', {'job': 'web'}, [(toDateTime64('2026-01-01 00:00:01', 3), 2.)]);
DETACH TABLE ts_prewarm_cols;
ATTACH TABLE ts_prewarm_cols;
INSERT INTO ts_prewarm_cols (metric_name, tags, samples) VALUES
    ('http_requests', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:02', 3), 3.)]),
    ('http_requests', {'job': 'web'}, [(toDateTime64('2026-01-01 00:00:03', 3), 4.)]);
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts_prewarm_cols);

DROP TABLE ts_prewarm, ts_prewarm_cols;
