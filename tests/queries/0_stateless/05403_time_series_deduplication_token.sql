-- Tags: no-async-insert, no-random-settings
-- One token covers several input blocks, and a repeat of that token adds no rows.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_dedup, dedup_tags, dedup_samples;

SELECT '--- three blocks with one token reach the tags and samples tables ---';

CREATE TABLE dedup_tags
(
    id Tuple(UInt64, UUID),
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time Nullable(DateTime64(3)),
    max_time Nullable(DateTime64(3))
) ENGINE = MergeTree ORDER BY (metric_name, id)
SETTINGS non_replicated_deduplication_window = 100;

CREATE TABLE dedup_samples
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64
) ENGINE = MergeTree ORDER BY (id, timestamp)
SETTINGS non_replicated_deduplication_window = 100;

CREATE TABLE ts_dedup ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000
    DATA dedup_samples TAGS dedup_tags;

INSERT INTO ts_dedup (metric_name, tags, samples)
SETTINGS insert_deduplication_token = 'batch', max_threads = 1, max_insert_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0
VALUES
    ('s0', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 0.)]),
    ('s1', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 1.)]),
    ('s2', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 2.)]);

SELECT count() FROM dedup_tags;
SELECT arraySort(groupArray(value)) FROM dedup_samples;

SELECT '--- the same token inserts no duplicate rows ---';

INSERT INTO ts_dedup (metric_name, tags, samples)
SETTINGS insert_deduplication_token = 'batch', max_threads = 1, max_insert_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0
VALUES
    ('s0', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 0.)]),
    ('s1', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 1.)]),
    ('s2', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 2.)]);

SELECT count() FROM dedup_tags;
SELECT arraySort(groupArray(value)) FROM dedup_samples;

SELECT '--- a different token inserts the samples again ---';

INSERT INTO ts_dedup (metric_name, tags, samples)
SETTINGS insert_deduplication_token = 'batch2', max_threads = 1, max_insert_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0
VALUES
    ('s0', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 0.)]),
    ('s1', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 1.)]),
    ('s2', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 2.)]);

SELECT count() FROM dedup_tags;
SELECT arraySort(groupArray(value)) FROM dedup_samples;

DROP TABLE ts_dedup, dedup_tags, dedup_samples;
