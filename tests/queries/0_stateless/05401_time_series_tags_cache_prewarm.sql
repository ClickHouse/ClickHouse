-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas: the replicas execute the `timeSeriesTags` table function without coordinating
-- with the initiator, so a `count()` answered from the primary key index alone is summed up once per replica.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_prewarm, ts_prewarm_cols, ts_prewarm_dup, ts_prewarm_run, ts_prewarm_exp;

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

SELECT '--- three copies of one series and one other series preload both ---';
CREATE TABLE ts_prewarm_dup ENGINE = TimeSeries SETTINGS tags_deduplication_cache_size_bytes = 0, tags_to_columns = {'series': 'series'} TAGS INNER ENGINE = MergeTree ORDER BY series;
INSERT INTO ts_prewarm_dup (metric_name, tags, samples) VALUES
    ('m', {'series': 'a'}, [(toDateTime64('2026-01-01 00:00:00', 3), 1.)]),
    ('m', {'series': 'a'}, [(toDateTime64('2026-01-01 00:00:01', 3), 1.)]),
    ('m', {'series': 'a'}, [(toDateTime64('2026-01-01 00:00:02', 3), 1.)]),
    ('m', {'series': 'b'}, [(toDateTime64('2026-01-01 00:00:03', 3), 1.)]);
OPTIMIZE TABLE ts_prewarm_dup FINAL;
ALTER TABLE ts_prewarm_dup MODIFY SETTING tags_deduplication_cache_size_bytes = 432;
DETACH TABLE ts_prewarm_dup;
ATTACH TABLE ts_prewarm_dup;
INSERT INTO ts_prewarm_dup (metric_name, tags, samples) VALUES ('m', {'series': 'b'}, [(toDateTime64('2026-01-01 00:00:04', 3), 1.)]);
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts_prewarm_dup);

SELECT '--- a long run of one series ends the preload before the series stored after it ---';
CREATE TABLE ts_prewarm_run ENGINE = TimeSeries SETTINGS tags_deduplication_cache_size_bytes = 0, tags_to_columns = {'series': 'series'} TAGS INNER ENGINE = MergeTree ORDER BY series;
INSERT INTO ts_prewarm_run (metric_name, tags, samples) SELECT 'm', map('series', 'a'), [(toDateTime64(number, 3), 1.)] FROM numbers(200000);
INSERT INTO ts_prewarm_run (metric_name, tags, samples) VALUES ('m', {'series': 'b'}, [(toDateTime64('2026-01-01 00:00:00', 3), 1.)]);
OPTIMIZE TABLE ts_prewarm_run FINAL;
ALTER TABLE ts_prewarm_run MODIFY SETTING tags_deduplication_cache_size_bytes = 432;
DETACH TABLE ts_prewarm_run;
ATTACH TABLE ts_prewarm_run;
INSERT INTO ts_prewarm_run (metric_name, tags, samples) VALUES
    ('m', {'series': 'a'}, [(toDateTime64('2026-01-01 00:00:01', 3), 1.)]),
    ('m', {'series': 'b'}, [(toDateTime64('2026-01-01 00:00:01', 3), 1.)]);
SELECT series, count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts_prewarm_run) GROUP BY series ORDER BY series;

SELECT '--- a large expiration still preloads the stored series ---';
CREATE TABLE ts_prewarm_exp ENGINE = TimeSeries SETTINGS tags_deduplication_cache_expiration_seconds = 10000000001 TAGS INNER ENGINE = MergeTree ORDER BY (metric_name, id);
INSERT INTO ts_prewarm_exp (metric_name, tags, samples) VALUES ('http_requests', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:00', 3), 1.)]);
DETACH TABLE ts_prewarm_exp;
ATTACH TABLE ts_prewarm_exp;
INSERT INTO ts_prewarm_exp (metric_name, tags, samples) VALUES ('http_requests', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:01', 3), 2.)]);
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts_prewarm_exp);

DROP TABLE ts_prewarm, ts_prewarm_cols, ts_prewarm_dup, ts_prewarm_run, ts_prewarm_exp;
