-- Tags: no-parallel, no-replicated-database
-- no-parallel: creates the global database `inner`.

-- A TimeSeries target in a database named `inner` is formatted to a query that parses back.
SELECT formatQuerySingleLine(formatQuerySingleLine('ATTACH TABLE ts ENGINE = TimeSeries TAGS `inner`.t'));
SELECT formatQuerySingleLine(formatQuerySingleLine('ATTACH TABLE ts ENGINE = TimeSeries SAMPLES `INNER`.s'));
SELECT formatQuerySingleLine(formatQuerySingleLine('ATTACH TABLE ts ENGINE = TimeSeries METRIC FAMILIES `inner`'));
SELECT formatQuerySingleLine(formatQuerySingleLine('ATTACH TABLE ts ENGINE = TimeSeries TAGS `inner`.t TAGS INNER ENGINE = Memory'));

-- The same for a real table: ATTACH reads back the stored metadata.
SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_05632;
DROP DATABASE IF EXISTS `inner`;
CREATE DATABASE `inner`;

CREATE TABLE `inner`.metric_families (metric_family String, type LowCardinality(String), unit LowCardinality(String), help String) ENGINE = Memory;
INSERT INTO `inner`.metric_families VALUES ('http_requests', 'counter', '', 'Total requests');

CREATE TABLE ts_05632 ENGINE = TimeSeries SETTINGS version = 9 METRIC FAMILIES `inner`.metric_families;
DETACH TABLE ts_05632;
ATTACH TABLE ts_05632;

SELECT metric_family, type, help FROM timeSeriesMetricFamilies(ts_05632);

DROP TABLE ts_05632;
DROP DATABASE `inner`;
