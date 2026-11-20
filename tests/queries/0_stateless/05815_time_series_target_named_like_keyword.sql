-- A TimeSeries target named like a keyword of an inner engine is formatted to a query that parses back to the same query.
SELECT formatQuerySingleLine(formatQuerySingleLine('ATTACH TABLE ts ENGINE = TimeSeries TAGS `inner` SETTINGS max_threads = 1'));
SELECT formatQuerySingleLine(formatQuerySingleLine('ATTACH TABLE ts ENGINE = TimeSeries SAMPLES s METRIC FAMILIES `Inner` SETTINGS max_threads = 1'));
SELECT formatQuerySingleLine(formatQuerySingleLine('ATTACH TABLE ts ENGINE = TimeSeries TAGS `engine` SAMPLES s'));
SELECT formatQuerySingleLine(formatQuerySingleLine('ATTACH TABLE ts ENGINE = TimeSeries TAGS `ttl` SETTINGS max_threads = 1'));
SELECT formatQuerySingleLine(formatQuerySingleLine('ATTACH TABLE ts ENGINE = TimeSeries TAGS `settings` COMMENT \'c\''));
SELECT formatQuerySingleLine(formatQuerySingleLine('ATTACH TABLE ts ENGINE = TimeSeries TAGS `inner`.t SETTINGS max_threads = 1'));
SELECT formatQuerySingleLine(formatQuerySingleLine('ATTACH TABLE ts ENGINE = TimeSeries TAGS INNER ENGINE = Memory'));
-- A materialized view `TO` target is not parsed as an inner engine, so its name is not backquoted.
SELECT formatQuerySingleLine(formatQuerySingleLine('CREATE MATERIALIZED VIEW mv TO `inner` AS SELECT 1'));
SELECT formatQuerySingleLine(formatQuerySingleLine('CREATE MATERIALIZED VIEW mv TO `ttl` AS SELECT 1'));

-- The same for a real query.
SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_05815;
DROP TABLE IF EXISTS `inner`;

CREATE TABLE `inner` (metric_family String, type LowCardinality(String), unit LowCardinality(String), help String) ENGINE = Memory;
INSERT INTO `inner` VALUES ('http_requests', 'counter', '', 'Total requests');

CREATE TABLE ts_05815 ENGINE = TimeSeries SETTINGS version = 9 METRIC FAMILIES `inner` SETTINGS max_threads = 1;

SELECT metric_family, type, help FROM timeSeriesMetricFamilies(ts_05815);

DROP TABLE ts_05815;
DROP TABLE `inner`;
