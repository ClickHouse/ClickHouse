-- Tags: no-async-insert, no-random-settings
-- One insert_deduplication_token covers several input blocks, and a repeat adds no rows.
-- Identical blocks without a token stay distinct.
-- A failed block keeps its token, and a retry fills the gap.

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
DROP TABLE IF EXISTS ts_partial, partial_tags, partial_samples;

SELECT '--- the failing insert keeps tags for the committed blocks ---';

CREATE TABLE partial_tags
(
    id Tuple(UInt64, UUID),
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time Nullable(DateTime64(3)),
    max_time Nullable(DateTime64(3))
) ENGINE = MergeTree ORDER BY (metric_name, id)
SETTINGS non_replicated_deduplication_window = 100;

CREATE TABLE partial_samples
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64,
    CONSTRAINT c CHECK value != 2
) ENGINE = MergeTree ORDER BY (id, timestamp)
SETTINGS non_replicated_deduplication_window = 100;

CREATE TABLE ts_partial ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000
    DATA partial_samples TAGS partial_tags;

INSERT INTO ts_partial (metric_name, tags, samples)
SETTINGS insert_deduplication_token = 'batch', max_threads = 1, max_insert_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0
VALUES
    ('a', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 1.)]),
    ('b', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 2.)]),
    ('c', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 3.)]); -- { serverError VIOLATED_CONSTRAINT }

SELECT arraySort(groupArray(metric_name)) FROM partial_tags;

SELECT '--- the same token inserts the rejected block after the constraint is gone ---';

ALTER TABLE partial_samples DROP CONSTRAINT c;

INSERT INTO ts_partial (metric_name, tags, samples)
SETTINGS insert_deduplication_token = 'batch', max_threads = 1, max_insert_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0
VALUES
    ('a', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 1.)]),
    ('b', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 2.)]),
    ('c', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 3.)]);

SELECT arraySort(groupArray(value)) FROM partial_samples;
SELECT arraySort(groupArray(metric_name)) FROM partial_tags;
SELECT metric_name FROM ts_partial ORDER BY metric_name;

DROP TABLE ts_partial, partial_tags, partial_samples;
