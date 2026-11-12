-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: PromQL requires ANTLR4, which is disabled in the fast-test build.
-- Tag no-replicated-database: DatabaseReplicated does not drop inner TimeSeries tables synchronously.

SET allow_experimental_time_series_table = 1;
SET time_series_prefer_recent_samples_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS t_05595_ts;
DROP TABLE IF EXISTS t_05595_samples;
DROP TABLE IF EXISTS t_05595_tags;
DROP TABLE IF EXISTS t_05595_metrics;
DROP TABLE IF EXISTS t_05595_owned;

-- Version 9 supports external targets and creates inner recent samples and time ranges tables by default.
CREATE TABLE t_05595_samples
(
    id UUID,
    timestamp DateTime64(3),
    value Float64
)
ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE t_05595_tags
(
    id UUID,
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String)
)
ENGINE = ReplacingMergeTree ORDER BY (metric_name, id);

CREATE TABLE t_05595_metrics
(
    metric_family String,
    type LowCardinality(String),
    unit LowCardinality(String),
    help String
)
ENGINE = ReplacingMergeTree ORDER BY metric_family;

CREATE TABLE t_05595_ts ENGINE = TimeSeries SETTINGS version = 9
SAMPLES t_05595_samples TAGS t_05595_tags METRIC FAMILIES t_05595_metrics;

INSERT INTO t_05595_ts (metric_name, tags, samples) VALUES
    ('m', map('env', 'a'), [(toDateTime64('2100-01-01 00:00:00', 3), 1.)]),
    ('m', map('env', 'b'), [(toDateTime64('2100-01-01 00:00:00', 3), 2.)]),
    ('m', map('env', 'c'), [(toDateTime64('2100-01-01 00:00:00', 3), 3.)]);

SELECT count() FROM t_05595_samples;
SELECT value FROM prometheusQuery(t_05595_ts, 'm', toDateTime64('2100-01-01 00:00:00', 3)) ORDER BY value;

-- These operations must not touch only the inner auxiliary tables.
TRUNCATE TABLE t_05595_ts; -- { serverError INCORRECT_QUERY }
OPTIMIZE TABLE t_05595_ts FINAL; -- { serverError INCORRECT_QUERY }

-- Rejected TRUNCATE must leave external data and the recent-samples read path intact.
SELECT count() FROM t_05595_samples;
SELECT value FROM prometheusQuery(t_05595_ts, 'm', toDateTime64('2100-01-01 00:00:00', 3)) ORDER BY value;

-- Fully owned TimeSeries tables can still be truncated.
CREATE TABLE t_05595_owned ENGINE = TimeSeries;
TRUNCATE TABLE t_05595_owned;
DROP TABLE t_05595_owned;

DROP TABLE t_05595_ts;
DROP TABLE t_05595_metrics;
DROP TABLE t_05595_tags;
DROP TABLE t_05595_samples;

-- An unsupported bulk truncate must reject before any table (including unrelated tables) is changed.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.samples
    (id UUID, timestamp DateTime64(3), value Float64)
    ENGINE = MergeTree ORDER BY (id, timestamp);
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.tags
    (id UUID, metric_name LowCardinality(String), tags Map(LowCardinality(String), String))
    ENGINE = ReplacingMergeTree ORDER BY (metric_name, id);
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.metrics
    (metric_family String, type LowCardinality(String), unit LowCardinality(String), help String)
    ENGINE = ReplacingMergeTree ORDER BY metric_family;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.unrelated (value UInt8) ENGINE = Memory;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.ts ENGINE = TimeSeries SETTINGS version = 9
    SAMPLES {CLICKHOUSE_DATABASE_1:Identifier}.samples
    TAGS {CLICKHOUSE_DATABASE_1:Identifier}.tags
    METRIC FAMILIES {CLICKHOUSE_DATABASE_1:Identifier}.metrics;

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.ts (metric_name, tags, samples) VALUES
    ('m', map('env', 'a'), [(toDateTime64('2100-01-01 00:00:00', 3), 1.)]);
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.unrelated VALUES (1);

TRUNCATE ALL TABLES FROM {CLICKHOUSE_DATABASE_1:Identifier}; -- { serverError INCORRECT_QUERY }
SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.samples;
SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.tags;
SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.unrelated;

-- Selecting just the external target must not leave the unselected wrapper's caches stale.
TRUNCATE ALL TABLES FROM {CLICKHOUSE_DATABASE_1:Identifier} LIKE 'samples'; -- { serverError INCORRECT_QUERY }
SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.samples;

-- Filters containing no affected TimeSeries tables are still allowed.
TRUNCATE ALL TABLES FROM {CLICKHOUSE_DATABASE_1:Identifier} LIKE 'unrelated';
SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.unrelated;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier} SYNC;
