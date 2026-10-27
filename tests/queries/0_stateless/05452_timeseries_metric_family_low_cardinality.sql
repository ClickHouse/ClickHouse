-- The metric families table of a `TimeSeries` table may use `LowCardinality` columns, including its
-- key column `metric_family`. Metadata inserted through the `TimeSeries` table must reach it, both
-- for an external target table (version 9, the last version supporting them; such tables used to be
-- rejected) and for an inner table customized with `METRIC FAMILIES INNER COLUMNS`.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_external;
DROP TABLE IF EXISTS ts_inner;
DROP TABLE IF EXISTS ts_samples;
DROP TABLE IF EXISTS ts_tags;
DROP TABLE IF EXISTS ts_metrics;

SELECT '-- external target table, version 9';

CREATE TABLE ts_samples (id UUID, timestamp DateTime64(3), value Float64)
ENGINE = MergeTree ORDER BY (id, timestamp);

CREATE TABLE ts_tags (id UUID, metric_name LowCardinality(String), tags Map(LowCardinality(String), String))
ENGINE = ReplacingMergeTree ORDER BY (metric_name, id);

CREATE TABLE ts_metrics
(
    metric_family LowCardinality(String),
    type LowCardinality(String),
    unit LowCardinality(String),
    help String
)
ENGINE = ReplacingMergeTree ORDER BY metric_family;

CREATE TABLE ts_external ENGINE = TimeSeries SETTINGS version = 9 SAMPLES ts_samples TAGS ts_tags METRICS ts_metrics;

INSERT INTO ts_external (metric_family, type, unit, help) VALUES ('cpu_usage', 'gauge', 'percent', 'CPU usage');

SELECT metric_family, type, unit, help FROM ts_metrics FINAL;
SELECT metric_family, type, unit, help FROM ts_external WHERE metric_family != '';

SELECT '-- inner table, current version';

CREATE TABLE ts_inner ENGINE = TimeSeries
METRIC FAMILIES INNER COLUMNS
(
    metric_family LowCardinality(String),
    type LowCardinality(String),
    unit LowCardinality(String),
    help String
);

INSERT INTO ts_inner (metric_family, type, unit, help) VALUES ('cpu_usage', 'gauge', 'percent', 'CPU usage');

SELECT metric_family, toTypeName(metric_family), type, unit, help FROM timeSeriesMetricFamilies(ts_inner);
SELECT metric_family, type, unit, help FROM ts_inner WHERE metric_family != '';

DROP TABLE ts_inner;
DROP TABLE ts_external;
DROP TABLE ts_metrics;
DROP TABLE ts_tags;
DROP TABLE ts_samples;
