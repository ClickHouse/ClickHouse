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
