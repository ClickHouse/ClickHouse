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

SELECT '--- a partitioned recent target keeps no rows when samples fail ---';

CREATE TABLE part_tags
(
    id Tuple(UInt64, UUID),
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time Nullable(DateTime64(3)),
    max_time Nullable(DateTime64(3))
) ENGINE = MergeTree ORDER BY (metric_name, id);

CREATE TABLE part_samples
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64,
    CONSTRAINT c CHECK sleepEachRow(1) = 0 AND value < 40
) ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE part_recent
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64
) ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (id, timestamp)
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

CREATE TABLE ts_part ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000
    DATA part_samples TAGS part_tags RECENT SAMPLES part_recent;

INSERT INTO ts_part (metric_name, tags, samples)
SETTINGS max_insert_delayed_streams_for_parallel_write = 0
VALUES ('m', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 42.), (toDateTime64('2020-01-02 00:00:00', 3), 43.)]); -- { serverError VIOLATED_CONSTRAINT }

SELECT count() FROM part_samples;
SELECT count() FROM part_recent;
SELECT count() FROM part_recent WHERE value = 42;

SELECT '--- max_insert_delayed_streams_for_parallel_write = 6 ---';

INSERT INTO ts_part (metric_name, tags, samples)
SETTINGS max_insert_delayed_streams_for_parallel_write = 6
VALUES ('m6', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 42.), (toDateTime64('2020-01-02 00:00:00', 3), 43.)]); -- { serverError VIOLATED_CONSTRAINT }

SELECT count() FROM part_samples;
SELECT count() FROM part_recent;
SELECT count() FROM part_recent WHERE value = 42;

SELECT '--- a Buffer recent target keeps no rows when samples fail ---';

CREATE TABLE buf_tags
(
    id Tuple(UInt64, UUID),
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time Nullable(DateTime64(3)),
    max_time Nullable(DateTime64(3))
) ENGINE = MergeTree ORDER BY (metric_name, id);

CREATE TABLE buf_samples
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64,
    CONSTRAINT c CHECK value < 40
) ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE buf_dest
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64
) ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE buf_recent
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64
) ENGINE = Buffer(currentDatabase(), buf_dest, 1, 86400, 86400, 10000000, 10000000, 100000000, 100000000);

CREATE TABLE ts_buf ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000
    DATA buf_samples TAGS buf_tags RECENT SAMPLES buf_recent;

INSERT INTO ts_buf (metric_name, tags, samples)
VALUES ('m', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 42.)]); -- { serverError VIOLATED_CONSTRAINT }

SELECT count() FROM buf_samples;
SELECT count() FROM buf_recent;
SELECT count() FROM buf_recent WHERE value = 42;

SELECT '--- a recent target stores a default column ---';

CREATE TABLE def_tags
(
    id Tuple(UInt64, UUID),
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time Nullable(DateTime64(3)),
    max_time Nullable(DateTime64(3))
) ENGINE = MergeTree ORDER BY (metric_name, id);

CREATE TABLE def_samples
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64
) ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE def_recent
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64,
    extra UInt8 DEFAULT 7
) ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE ts_def ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000
    DATA def_samples TAGS def_tags RECENT SAMPLES def_recent;

INSERT INTO ts_def (metric_name, tags, samples) VALUES ('m', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 1.)]);

SELECT extra FROM def_recent;

SELECT '--- a Distributed recent target keeps no rows when samples fail ---';

CREATE TABLE dist_tags
(
    id Tuple(UInt64, UUID),
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time Nullable(DateTime64(3)),
    max_time Nullable(DateTime64(3))
) ENGINE = MergeTree ORDER BY (metric_name, id);

CREATE TABLE dist_samples
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64,
    CONSTRAINT c CHECK sleepEachRow(1) = 0 AND value < 40
) ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE dist_recent_dest
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64
) ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE dist_recent
(
    id Tuple(UInt64, UUID),
    timestamp DateTime64(3),
    value Float64
) ENGINE = Distributed(test_shard_localhost, currentDatabase(), dist_recent_dest);

CREATE TABLE ts_dist ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000
    DATA dist_samples TAGS dist_tags RECENT SAMPLES dist_recent;

INSERT INTO ts_dist (metric_name, tags, samples)
SETTINGS insert_distributed_sync = 1
VALUES ('m', map('env', 'prod'), [(toDateTime64('2020-01-01 00:00:00', 3), 42.)]); -- { serverError VIOLATED_CONSTRAINT }

SELECT count() FROM dist_samples;
SELECT count() FROM dist_recent_dest;
SELECT count() FROM dist_recent_dest WHERE value = 42;

DROP VIEW fail_mv;
DROP TABLE ts_fail, ts_fail_mem, fail_tags, fail_samples, fail_recent, fail_mv_dest, mem_tags, mem_samples, mem_recent;
DROP TABLE ts_part, part_tags, part_samples, part_recent;
DROP TABLE ts_buf, buf_tags, buf_samples, buf_recent, buf_dest;
DROP TABLE ts_def, def_tags, def_samples, def_recent;
DROP TABLE ts_dist, dist_tags, dist_samples, dist_recent, dist_recent_dest;
