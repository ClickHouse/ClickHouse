-- Tags: no-async-insert, no-random-settings
-- A samples failure commits no recent rows, and a dependent view does not receive that block.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_fail, ts_fail_mem, fail_tags, fail_samples, fail_recent, fail_mv_dest, mem_tags, mem_samples, mem_recent;

SELECT '--- a failed samples write leaves no recent rows and no view rows ---';

CREATE TABLE fail_tags
(
    id Tuple(UInt64, UUID),
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time Nullable(DateTime64(3)),
    max_time Nullable(DateTime64(3))
) ENGINE = MergeTree ORDER BY (metric_name, id);

CREATE TABLE fail_samples
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64,
    CONSTRAINT c CHECK value < 40
) ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE fail_recent
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64
) ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE ts_fail ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000
    DATA fail_samples TAGS fail_tags RECENT SAMPLES fail_recent;

CREATE TABLE fail_mv_dest (metric_name String) ENGINE = MergeTree ORDER BY metric_name;
CREATE MATERIALIZED VIEW fail_mv TO fail_mv_dest AS SELECT metric_name FROM ts_fail;

INSERT INTO ts_fail (metric_name, tags, samples) VALUES ('ok', map('env', 'prod'), [(now64(3) - INTERVAL 1 MINUTE, 1.)]);

INSERT INTO ts_fail (metric_name, tags, samples) VALUES ('bad', map('env', 'prod'), [(now64(3) - INTERVAL 1 MINUTE, 42.)]); -- { serverError VIOLATED_CONSTRAINT }

SELECT count() FROM fail_tags WHERE metric_name = 'bad';
SELECT count() FROM fail_samples WHERE value >= 40;
SELECT count() FROM fail_recent WHERE value >= 40;
SELECT count() FROM fail_mv_dest WHERE metric_name = 'bad';
SELECT metric_name FROM fail_mv_dest ORDER BY metric_name;

SELECT '--- max_threads = 1 ---';

INSERT INTO ts_fail (metric_name, tags, samples) SETTINGS max_threads = 1 VALUES ('bad_sync', map('env', 'prod'), [(now64(3) - INTERVAL 1 MINUTE, 41.)]); -- { serverError VIOLATED_CONSTRAINT }

SELECT count() FROM fail_samples WHERE value >= 40;
SELECT count() FROM fail_recent WHERE value >= 40;
SELECT count() FROM fail_mv_dest WHERE metric_name = 'bad_sync';

SELECT '--- Memory engine recent samples stay empty when samples fail ---';

CREATE TABLE mem_tags
(
    id Tuple(UInt64, UUID),
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time Nullable(DateTime64(3)),
    max_time Nullable(DateTime64(3))
) ENGINE = Memory;

CREATE TABLE mem_samples
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64,
    CONSTRAINT c CHECK value < 40
) ENGINE = Memory;

CREATE TABLE mem_recent (id Tuple(UInt64, UUID), timestamp DateTime64(3), value Float64) ENGINE = Memory;

CREATE TABLE ts_fail_mem ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000
    DATA mem_samples TAGS mem_tags RECENT SAMPLES mem_recent;

INSERT INTO ts_fail_mem (metric_name, tags, samples) VALUES ('bad_mem', map('env', 'prod'), [(now64(3) - INTERVAL 1 MINUTE, 42.)]); -- { serverError VIOLATED_CONSTRAINT }

SELECT count() FROM mem_samples WHERE value >= 40;
SELECT count() FROM mem_recent WHERE value >= 40;

SELECT '--- a later block failure leaves no recent rows for that block ---';

INSERT INTO ts_fail (metric_name, tags, samples)
SELECT if(number = 0, 'good_block', 'bad_block'), map('env', 'prod'), [(now64(3) - INTERVAL 1 MINUTE, if(number = 0, toFloat64(1), toFloat64(42)))]
FROM numbers(2)
SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0, max_insert_block_size = 1; -- { serverError VIOLATED_CONSTRAINT }

SELECT value FROM fail_samples ORDER BY value;
SELECT value FROM fail_recent ORDER BY value;
SELECT count() FROM fail_mv_dest WHERE metric_name = 'bad_block';

DROP VIEW fail_mv;
DROP TABLE ts_fail, ts_fail_mem, fail_tags, fail_samples, fail_recent, fail_mv_dest, mem_tags, mem_samples, mem_recent;
