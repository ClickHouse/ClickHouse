-- Tags: no-async-insert, no-random-settings
-- Identical blocks stay distinct. A later insert with different rows is not dropped. A retry adds nothing.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_tokenless, tokenless_tags, tokenless_samples;

SELECT '--- three identical blocks without a token all land ---';

CREATE TABLE tokenless_tags
(
    id Tuple(UInt64, UUID),
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time Nullable(DateTime64(3)),
    max_time Nullable(DateTime64(3))
) ENGINE = MergeTree ORDER BY (metric_name, id)
SETTINGS non_replicated_deduplication_window = 100;

CREATE TABLE tokenless_samples
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64
) ENGINE = MergeTree ORDER BY (id, timestamp)
SETTINGS non_replicated_deduplication_window = 100;

CREATE TABLE ts_tokenless ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000
    DATA tokenless_samples TAGS tokenless_tags;

INSERT INTO ts_tokenless (metric_name, tags, samples)
SETTINGS deduplicate_insert = 'enable', max_threads = 1, max_insert_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0
VALUES
    ('same', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 1.)]),
    ('same', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 1.)]),
    ('same', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 1.)]);

SELECT count() FROM tokenless_samples;
SELECT arraySort(groupArray(value)) FROM tokenless_samples;

SELECT '--- a different insert is not collapsed ---';

INSERT INTO ts_tokenless (metric_name, tags, samples)
SETTINGS deduplicate_insert = 'enable', max_threads = 1, max_insert_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0
VALUES
    ('other', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 9.)]);

SELECT count() FROM tokenless_samples;
SELECT arraySort(groupArray(value)) FROM tokenless_samples;

SELECT '--- a retry of the first insert adds no rows ---';

INSERT INTO ts_tokenless (metric_name, tags, samples)
SETTINGS deduplicate_insert = 'enable', max_threads = 1, max_insert_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0
VALUES
    ('same', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 1.)]),
    ('same', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 1.)]),
    ('same', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 1.)]);

SELECT count() FROM tokenless_samples;
SELECT arraySort(groupArray(value)) FROM tokenless_samples;

DROP TABLE ts_tokenless, tokenless_tags, tokenless_samples;
