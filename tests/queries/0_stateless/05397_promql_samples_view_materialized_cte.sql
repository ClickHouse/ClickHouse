-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- The samples view reads one materialized CTE twice. PromQL must return its row.

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS prometheus;
DROP TABLE IF EXISTS tags_table;
DROP VIEW IF EXISTS samples_view;

CREATE VIEW samples_view SQL SECURITY INVOKER AS
WITH c AS MATERIALIZED
(
    SELECT number AS id,
           toDateTime64(100 + number, 3) AS timestamp,
           toFloat64(number + 1) AS value
    FROM numbers(3)
)
SELECT * FROM c WHERE value = (SELECT max(value) FROM c);

CREATE TABLE tags_table
(
    id UInt64,
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time DateTime64(3),
    max_time DateTime64(3)
) ENGINE = MergeTree() ORDER BY id;

INSERT INTO tags_table VALUES (2, 'm', map(), toDateTime64(100, 3), toDateTime64(110, 3));

CREATE TABLE prometheus ENGINE = TimeSeries
SETTINGS recent_samples_ttl_seconds = 0
SAMPLES samples_view
TAGS tags_table;

SELECT value FROM prometheusQuery('prometheus', 'm', 102);

DROP TABLE prometheus;
DROP TABLE tags_table;
DROP VIEW samples_view;
