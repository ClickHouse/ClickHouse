-- Tags: no-async-insert, no-random-settings
-- A samples constraint stops the insert after earlier blocks commit. The same token fills the gap.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

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
