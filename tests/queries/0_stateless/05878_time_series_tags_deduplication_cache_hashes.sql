SET enable_time_series_table = 1;
SET enable_parallel_replicas = 0;
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS ts_default, ts_large_dict, ts_tuple_syntax, ts_cols, ts_custom_u64, ts_custom_tuple, ts_custom_string, ts_uuid, ts_u128, ts_fixed16, ts_v8;

SELECT '--- default id skips duplicates and distinguishes metrics sharing non-name tags ---';
CREATE TABLE ts_default ENGINE = TimeSeries TAGS INNER ENGINE = MergeTree ORDER BY (metric_name, id);
INSERT INTO ts_default (metric_name, tags, samples) VALUES
    ('m1', {'job': 'api', 'instance': 'h1'}, [(toDateTime64('2026-01-01 00:00:00', 3), 1.)]),
    ('m2', {'job': 'api', 'instance': 'h1'}, [(toDateTime64('2026-01-01 00:00:00', 3), 2.)]),
    ('m1', {'job': 'web', 'instance': 'h1'}, [(toDateTime64('2026-01-01 00:00:00', 3), 3.)]),
    ('m1', {'job': 'api', 'instance': 'h1'}, [(toDateTime64('2026-01-01 00:00:01', 3), 4.)]);
SELECT metric_name, tags['job'], count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts_default) GROUP BY metric_name, tags['job'] ORDER BY metric_name, tags['job'];

INSERT INTO ts_default (metric_name, tags, samples) SETTINGS async_insert = 0, log_queries = 1, log_comment = '05878_default_warm' VALUES
    ('m1', {'job': 'api', 'instance': 'h1'}, [(toDateTime64('2026-01-01 00:00:02', 3), 5.)]),
    ('m2', {'job': 'api', 'instance': 'h1'}, [(toDateTime64('2026-01-01 00:00:02', 3), 6.)]),
    ('m1', {'job': 'web', 'instance': 'h1'}, [(toDateTime64('2026-01-01 00:00:02', 3), 7.)]),
    ('m2', {'job': 'web', 'instance': 'h1'}, [(toDateTime64('2026-01-01 00:00:02', 3), 8.)]);
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts_default);
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['TimeSeriesTagsDeduplicationCacheHits'], ProfileEvents['TimeSeriesTagsDeduplicationCacheMisses']
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05878_default_warm';

INSERT INTO ts_default (metric_name, tags, samples) SETTINGS async_insert = 1, wait_for_async_insert = 1 VALUES
    ('m1', {'job': 'api', 'instance': 'h1'}, [(toDateTime64('2026-01-01 00:00:03', 3), 9.)]),
    ('m2', {'job': 'web', 'instance': 'h1'}, [(toDateTime64('2026-01-01 00:00:03', 3), 10.)]);
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts_default);

SELECT '--- default id with 70000 series in one block exercises UInt8, UInt16, and UInt32 LowCardinality indexes ---';
CREATE TABLE ts_large_dict ENGINE = TimeSeries
SETTINGS tags_deduplication_cache_size_bytes = 33554432, recent_samples_ttl_seconds = 0, store_time_ranges = 0
TAGS INNER ENGINE = MergeTree ORDER BY (metric_name, id);
INSERT INTO ts_large_dict (metric_name, tags, samples)
SETTINGS max_block_size = 100000, min_insert_block_size_rows = 100000
SELECT 'm_shared', map('series', toString(number)), [] FROM numbers(70000);
INSERT INTO ts_large_dict (metric_name, tags, samples)
SETTINGS max_block_size = 100000, min_insert_block_size_rows = 100000, log_queries = 1, log_comment = '05878_large_dict_warm'
SELECT 'm_shared', map('series', toString(number)), [] FROM numbers(70000);
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts_large_dict);
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['TimeSeriesTagsDeduplicationCacheHits'], ProfileEvents['TimeSeriesTagsDeduplicationCacheMisses']
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05878_large_dict_warm';

SELECT '--- explicit parenthesized tuple default expression matches canonical default ---';
CREATE TABLE ts_tuple_syntax ENGINE = TimeSeries
TAGS INNER COLUMNS (
    id Tuple(UInt64, LowCardinality(UUID)) DEFAULT (sipHash64(metric_name), toLowCardinality(reinterpretAsUUID(sipHash128(tags))))
)
TAGS INNER ENGINE = MergeTree ORDER BY (metric_name, id);
INSERT INTO ts_tuple_syntax (metric_name, tags, samples) VALUES
    ('m1', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:00', 3), 1.)]),
    ('m2', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:00', 3), 2.)]);
INSERT INTO ts_tuple_syntax (metric_name, tags, samples) VALUES
    ('m1', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:01', 3), 3.)]),
    ('m2', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:01', 3), 4.)]);
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts_tuple_syntax);

SELECT '--- default id with tags_to_columns ---';
CREATE TABLE ts_cols ENGINE = TimeSeries SETTINGS tags_to_columns = {'job': 'job', 'region': 'region'} TAGS INNER ENGINE = MergeTree ORDER BY (metric_name, id);
INSERT INTO ts_cols (metric_name, tags, samples) VALUES
    ('http_requests', {'job': 'api', 'region': 'us'}, [(toDateTime64('2026-01-01 00:00:00', 3), 1.)]),
    ('http_requests', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:00', 3), 2.)]);
INSERT INTO ts_cols (metric_name, tags, samples) VALUES
    ('http_requests', {'job': 'api', 'region': 'us'}, [(toDateTime64('2026-01-01 00:00:01', 3), 3.)]),
    ('http_requests', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:01', 3), 4.)]);
SELECT metric_name, job, region, count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts_cols) GROUP BY metric_name, job, region ORDER BY region;

SELECT '--- custom UInt64 id expression keeps row hash when two tag sets share an id ---';
CREATE TABLE ts_custom_u64 ENGINE = TimeSeries
TAGS INNER COLUMNS (id UInt64 DEFAULT sipHash64(metric_name))
TAGS INNER ENGINE = MergeTree ORDER BY (metric_name, id);
INSERT INTO ts_custom_u64 (metric_name, tags, samples) VALUES ('http_requests', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:00', 3), 1.)]);
INSERT INTO ts_custom_u64 (metric_name, tags, samples) VALUES ('http_requests', {'job': 'web'}, [(toDateTime64('2026-01-01 00:00:01', 3), 2.)]);
INSERT INTO ts_custom_u64 (metric_name, tags, samples) VALUES ('http_requests', {'job': 'web'}, [(toDateTime64('2026-01-01 00:00:02', 3), 3.)]);
SELECT id = sipHash64(metric_name), tags['job'], count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts_custom_u64) GROUP BY id, metric_name, tags['job'] ORDER BY tags['job'];
INSERT INTO ts_custom_u64 (metric_name, tags, samples) VALUES
    ('rpc_calls', {'job': 'a'}, [(toDateTime64('2026-01-01 00:00:00', 3), 1.)]),
    ('rpc_calls', {'job': 'b'}, [(toDateTime64('2026-01-01 00:00:01', 3), 2.)]),
    ('rpc_calls', {'job': 'b'}, [(toDateTime64('2026-01-01 00:00:02', 3), 3.)]);
SELECT tags['job'], count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts_custom_u64) WHERE metric_name = 'rpc_calls' GROUP BY tags['job'] ORDER BY tags['job'];

SELECT '--- custom Tuple(UInt64, LowCardinality(UUID)) expression keeps row hash ---';
CREATE TABLE ts_custom_tuple ENGINE = TimeSeries
TAGS INNER COLUMNS (
    id Tuple(UInt64, LowCardinality(UUID)) DEFAULT tuple(sipHash64(metric_name), toLowCardinality(toUUID('00000000-0000-0000-0000-000000000001')))
)
TAGS INNER ENGINE = MergeTree ORDER BY (metric_name, id);
INSERT INTO ts_custom_tuple (metric_name, tags, samples) VALUES ('http_requests', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:00', 3), 1.)]);
INSERT INTO ts_custom_tuple (metric_name, tags, samples) VALUES ('http_requests', {'job': 'web'}, [(toDateTime64('2026-01-01 00:00:01', 3), 2.)]);
INSERT INTO ts_custom_tuple (metric_name, tags, samples) VALUES ('http_requests', {'job': 'web'}, [(toDateTime64('2026-01-01 00:00:02', 3), 3.)]);
SELECT tags['job'], count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts_custom_tuple) GROUP BY tags['job'] ORDER BY tags['job'];

SELECT '--- non-hash String id type keeps row hash ---';
CREATE TABLE ts_custom_string ENGINE = TimeSeries
SAMPLES INNER COLUMNS (id String)
RECENT SAMPLES INNER COLUMNS (id String)
TAGS INNER COLUMNS (id String DEFAULT metric_name)
TIME RANGES INNER COLUMNS (id String)
TAGS INNER ENGINE = MergeTree ORDER BY (metric_name, id);
INSERT INTO ts_custom_string (metric_name, tags, samples) VALUES ('http_requests', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:00', 3), 1.)]);
INSERT INTO ts_custom_string (metric_name, tags, samples) VALUES ('http_requests', {'job': 'web'}, [(toDateTime64('2026-01-01 00:00:01', 3), 2.)]);
INSERT INTO ts_custom_string (metric_name, tags, samples) VALUES ('http_requests', {'job': 'web'}, [(toDateTime64('2026-01-01 00:00:02', 3), 3.)]);
SELECT tags['job'], count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts_custom_string) GROUP BY tags['job'] ORDER BY tags['job'];

SELECT '--- single-component and tuple default UUID, UInt128, and FixedString(16) ids ---';
CREATE TABLE ts_uuid ENGINE = TimeSeries
SAMPLES INNER COLUMNS (id UUID)
RECENT SAMPLES INNER COLUMNS (id UUID)
TAGS INNER COLUMNS (id UUID)
TIME RANGES INNER COLUMNS (id UUID)
TAGS INNER ENGINE = MergeTree ORDER BY (metric_name, id);
INSERT INTO ts_uuid (metric_name, tags, samples) VALUES ('m1', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:00', 3), 1.)]), ('m2', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:00', 3), 2.)]);
INSERT INTO ts_uuid (metric_name, tags, samples) VALUES ('m1', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:01', 3), 3.)]), ('m2', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:01', 3), 4.)]);
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts_uuid);

CREATE TABLE ts_u128 ENGINE = TimeSeries
SAMPLES INNER COLUMNS (id Tuple(UUID, LowCardinality(UInt128)))
RECENT SAMPLES INNER COLUMNS (id Tuple(UUID, LowCardinality(UInt128)))
TAGS INNER COLUMNS (id Tuple(UUID, LowCardinality(UInt128)))
TIME RANGES INNER COLUMNS (id Tuple(UUID, LowCardinality(UInt128)))
TAGS INNER ENGINE = MergeTree ORDER BY (metric_name, id);
INSERT INTO ts_u128 (metric_name, tags, samples) VALUES ('m1', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:00', 3), 1.)]), ('m2', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:00', 3), 2.)]);
INSERT INTO ts_u128 (metric_name, tags, samples) VALUES ('m1', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:01', 3), 3.)]), ('m2', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:01', 3), 4.)]);
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts_u128);

CREATE TABLE ts_fixed16 ENGINE = TimeSeries
SAMPLES INNER COLUMNS (id LowCardinality(FixedString(16)))
RECENT SAMPLES INNER COLUMNS (id LowCardinality(FixedString(16)))
TAGS INNER COLUMNS (id LowCardinality(FixedString(16)))
TIME RANGES INNER COLUMNS (id LowCardinality(FixedString(16)))
TAGS INNER ENGINE = MergeTree ORDER BY (metric_name, id);
INSERT INTO ts_fixed16 (metric_name, tags, samples) VALUES ('m1', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:00', 3), 1.)]), ('m1', {'job': 'web'}, [(toDateTime64('2026-01-01 00:00:00', 3), 2.)]);
INSERT INTO ts_fixed16 (metric_name, tags, samples) VALUES ('m1', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:01', 3), 3.)]), ('m1', {'job': 'web'}, [(toDateTime64('2026-01-01 00:00:01', 3), 4.)]);
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts_fixed16);

SELECT '--- version 8 ALTER id_generator switches to row hash ---';
CREATE TABLE ts_v8 ENGINE = TimeSeries SETTINGS version = 8, id_type = 'UInt64' TAGS INNER ENGINE = MergeTree ORDER BY (metric_name, id);
INSERT INTO ts_v8 (metric_name, tags, samples) VALUES ('m1', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:00', 3), 1.)]);
INSERT INTO ts_v8 (metric_name, tags, samples) VALUES ('m1', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:01', 3), 2.)]);
SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts_v8);
ALTER TABLE ts_v8 MODIFY SETTING id_generator = 'sipHash64(metric_name)';
INSERT INTO ts_v8 (metric_name, tags, samples) VALUES ('m1', {'job': 'api'}, [(toDateTime64('2026-01-01 00:00:02', 3), 3.)]);
INSERT INTO ts_v8 (metric_name, tags, samples) VALUES ('m1', {'job': 'web'}, [(toDateTime64('2026-01-01 00:00:03', 3), 4.)]);
INSERT INTO ts_v8 (metric_name, tags, samples) VALUES ('m1', {'job': 'web'}, [(toDateTime64('2026-01-01 00:00:04', 3), 5.)]);
SELECT tags['job'], count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts_v8) GROUP BY tags['job'] ORDER BY tags['job'];

DROP TABLE ts_default, ts_large_dict, ts_tuple_syntax, ts_cols, ts_custom_u64, ts_custom_tuple, ts_custom_string, ts_uuid, ts_u128, ts_fixed16, ts_v8;
