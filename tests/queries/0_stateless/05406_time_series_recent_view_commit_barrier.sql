-- Tags: no-async-insert, no-random-settings
-- A dependent view on recent samples receives no rows when the samples write fails.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP VIEW IF EXISTS view_recent_mv;
DROP TABLE IF EXISTS ts_view_barrier, view_barrier_tags, view_barrier_samples, view_barrier_recent, view_barrier_buf, view_barrier_mem;

SELECT '--- rejected sample ---';

CREATE TABLE view_barrier_tags
(
    id Tuple(UInt64, UUID),
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time Nullable(DateTime64(3)),
    max_time Nullable(DateTime64(3))
) ENGINE = MergeTree ORDER BY (metric_name, id);

CREATE TABLE view_barrier_samples
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64,
    CONSTRAINT c CHECK sleepEachRow(2) = 0 AND value < 40
) ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE view_barrier_recent
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64
) ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE view_barrier_mem
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64
) ENGINE = Memory;

CREATE TABLE view_barrier_buf
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64
) ENGINE = Buffer(currentDatabase(), view_barrier_mem, 1, 1000, 1000, 1000, 1000, 1000000, 1000000);

CREATE MATERIALIZED VIEW view_recent_mv TO view_barrier_buf AS SELECT id, timestamp, value FROM view_barrier_recent;

CREATE TABLE ts_view_barrier ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000
    DATA view_barrier_samples TAGS view_barrier_tags RECENT SAMPLES view_barrier_recent;

INSERT INTO ts_view_barrier (metric_name, tags, samples) SETTINGS max_threads = 4, parallel_view_processing = 1, max_block_size = 1 VALUES ('m', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 42.)]); -- { serverError VIOLATED_CONSTRAINT }

SELECT count() FROM view_barrier_samples;
SELECT count() FROM view_barrier_recent;
SELECT count() FROM view_barrier_buf;

SELECT '--- accepted sample ---';

INSERT INTO ts_view_barrier (metric_name, tags, samples) SETTINGS max_threads = 4, parallel_view_processing = 1, max_block_size = 1 VALUES ('m', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 1.)]);

SELECT count() FROM view_barrier_samples;
SELECT count() FROM view_barrier_recent;
SELECT count() FROM view_barrier_buf;

DROP VIEW view_recent_mv;
DROP TABLE ts_view_barrier, view_barrier_tags, view_barrier_samples, view_barrier_recent, view_barrier_buf, view_barrier_mem;
