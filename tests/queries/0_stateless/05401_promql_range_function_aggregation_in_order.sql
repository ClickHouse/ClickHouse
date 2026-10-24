-- Tags: no-fasttest, no-parallel-replicas, no-random-settings, no-random-merge-tree-settings
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: the pipeline checks below expect a single local read of the samples table.
-- Tag no-random-settings, no-random-merge-tree-settings: the pipeline checks below expect the default block and granule sizes.

-- A function over a range vector (rate, increase, last_over_time, ...) aggregates the raw samples per series.
-- The samples table is sorted by (id, timestamp), so the first level of the aggregation runs in that order
-- and holds the state of one id at a time; the second level merges the ids of one set of tags.

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;
SET max_threads = 4;

DROP TABLE IF EXISTS prometheus;
CREATE TABLE prometheus ENGINE = TimeSeries;

-- Two inserts make two parts with overlapping time ranges, so the in-order read merges them.
INSERT INTO prometheus (metric_name, tags, samples) VALUES
    ('m', map('host', 'h1'), [(toDateTime64(100, 3), 10), (toDateTime64(110, 3), 20)]),
    ('m', map('host', 'h2'), [(toDateTime64(100, 3), 5), (toDateTime64(110, 3), 15)]);
INSERT INTO prometheus (metric_name, tags, samples) VALUES
    ('m', map('host', 'h1'), [(toDateTime64(120, 3), 30), (toDateTime64(130, 3), 40)]),
    ('m', map('host', 'h2'), [(toDateTime64(120, 3), 25), (toDateTime64(130, 3), 35)]);

SELECT '-- the aggregation over the raw samples runs in order (UUID ids)';
SELECT countIf(explain LIKE '%AggregatingInOrderTransform%') > 0 AS in_order
FROM (EXPLAIN PIPELINE SELECT * FROM prometheusQueryRange('prometheus', 'sum(rate(m[20s]))', 110, 130, 10));

SELECT '-- the in-order aggregation can be switched off by the setting';
SELECT countIf(explain LIKE '%AggregatingInOrderTransform%') AS in_order
FROM (EXPLAIN PIPELINE SELECT * FROM prometheusQueryRange('prometheus', 'sum(rate(m[20s]))', 110, 130, 10)
      SETTINGS optimize_aggregation_in_order = 0);

SELECT '-- sum(rate(m[20s])), range';
SELECT * FROM prometheusQueryRange('prometheus', 'sum(rate(m[20s]))', 110, 130, 10) ORDER BY tags;

SELECT '-- sum by (host) (rate(m[20s])), range';
SELECT * FROM prometheusQueryRange('prometheus', 'sum by (host) (rate(m[20s]))', 110, 130, 10) ORDER BY tags;

SELECT '-- rate(m[20s]), range';
SELECT * FROM prometheusQueryRange('prometheus', 'rate(m[20s])', 110, 130, 10) ORDER BY tags;

SELECT '-- m, range';
SELECT * FROM prometheusQueryRange('prometheus', 'm', 100, 130, 10) ORDER BY tags;

SELECT '-- rate(m[20s] offset 10s), instant';
SELECT * FROM prometheusQuery('prometheus', 'rate(m[20s] offset 10s)', 130) ORDER BY tags;

SELECT '-- m[20s], instant';
SELECT * FROM prometheusQuery('prometheus', 'm[20s]', 130) ORDER BY tags;

SELECT '-- quantile_over_time(0.5, m[30s]), instant';
SELECT * FROM prometheusQuery('prometheus', 'quantile_over_time(0.5, m[30s])', 130) ORDER BY tags;

SELECT '-- predict_linear(m[30s], 10), instant';
SELECT * FROM prometheusQuery('prometheus', 'predict_linear(m[30s], 10)', 130) ORDER BY tags;

SELECT '-- the results do not depend on the setting';
SELECT (SELECT groupArray((tags, samples)) FROM (SELECT * FROM prometheusQueryRange('prometheus', 'sum by (host) (rate(m[20s]))', 110, 130, 10) ORDER BY tags))
     = (SELECT groupArray((tags, samples)) FROM (SELECT * FROM prometheusQueryRange('prometheus', 'sum by (host) (rate(m[20s]))', 110, 130, 10) ORDER BY tags) SETTINGS optimize_aggregation_in_order = 0);

DROP TABLE prometheus;

-- The same with UInt64 ids in external target tables.
DROP TABLE IF EXISTS tags_table;
DROP TABLE IF EXISTS time_ranges_table;
DROP TABLE IF EXISTS samples_table;

CREATE TABLE tags_table
(
    id UInt64,
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String)
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE time_ranges_table
(
    id UInt64,
    min_time DateTime64(3),
    max_time DateTime64(3)
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE samples_table
(
    id UInt64,
    timestamp DateTime64(3),
    value Float64
) ENGINE = MergeTree() ORDER BY (id, timestamp);

CREATE TABLE prometheus ENGINE = TimeSeries SAMPLES samples_table TAGS tags_table TIME RANGES time_ranges_table;

INSERT INTO prometheus (metric_name, tags, samples) VALUES
    ('m', map('host', 'h1'), [(toDateTime64(100, 3), 10), (toDateTime64(110, 3), 20)]),
    ('m', map('host', 'h2'), [(toDateTime64(100, 3), 5), (toDateTime64(110, 3), 15)]);
INSERT INTO prometheus (metric_name, tags, samples) VALUES
    ('m', map('host', 'h1'), [(toDateTime64(120, 3), 30), (toDateTime64(130, 3), 40)]),
    ('m', map('host', 'h2'), [(toDateTime64(120, 3), 25), (toDateTime64(130, 3), 35)]);

SELECT '-- the aggregation over the raw samples runs in order (UInt64 ids)';
SELECT countIf(explain LIKE '%AggregatingInOrderTransform%') > 0 AS in_order
FROM (EXPLAIN PIPELINE SELECT * FROM prometheusQueryRange('prometheus', 'sum(rate(m[20s]))', 110, 130, 10));

SELECT '-- sum by (host) (rate(m[20s])), range, UInt64 ids';
SELECT * FROM prometheusQueryRange('prometheus', 'sum by (host) (rate(m[20s]))', 110, 130, 10) ORDER BY tags;

DROP TABLE prometheus;
DROP TABLE samples_table;
DROP TABLE time_ranges_table;
DROP TABLE tags_table;

-- Many series: each series is a counter with one sample per second, so the rate of every series is 1 and the sum is the number of series.
-- Four inserts make four parts with overlapping ranges of ids.
DROP TABLE IF EXISTS prometheus_big;
CREATE TABLE prometheus_big ENGINE = TimeSeries;

INSERT INTO prometheus_big (metric_name, tags, samples)
SELECT 'big', map('inst', toString(number)), arrayMap(i -> (toDateTime64(i, 3), toFloat64(i)), range(0, 300))
FROM numbers(500);
INSERT INTO prometheus_big (metric_name, tags, samples)
SELECT 'big', map('inst', toString(number)), arrayMap(i -> (toDateTime64(i, 3), toFloat64(i)), range(300, 600))
FROM numbers(500);
INSERT INTO prometheus_big (metric_name, tags, samples)
SELECT 'big', map('inst', toString(number)), arrayMap(i -> (toDateTime64(i, 3), toFloat64(i)), range(600, 900))
FROM numbers(500);
INSERT INTO prometheus_big (metric_name, tags, samples)
SELECT 'big', map('inst', toString(number)), arrayMap(i -> (toDateTime64(i, 3), toFloat64(i)), range(900, 1200))
FROM numbers(500);

-- The table is small, so a lower threshold per stream keeps four reading streams.
SET merge_tree_min_rows_for_concurrent_read = 1024;
SET merge_tree_min_bytes_for_concurrent_read = 1024;

-- The samples are split into primary key ranges, up to one per thread. Each range is aggregated on its own thread
-- and the streams are not merged, because the ranges hold disjoint series. The number of ranges depends on the marks.
SELECT '-- the in-order aggregation runs over primary key ranges';
SELECT countIf(explain LIKE '%FilterSortedStreamByRange%') > 1 AS split,
       countIf(explain LIKE '%FinishAggregatingInOrderTransform%') AS merging
FROM (EXPLAIN PIPELINE SELECT * FROM prometheusQueryRange('prometheus_big', 'sum(rate(big[60s]))', 60, 1200, 10));

SELECT countIf(explain LIKE '%Skip merging: 1%') > 0 AS skip_merging
FROM (EXPLAIN actions = 1 SELECT * FROM prometheusQueryRange('prometheus_big', 'sum(rate(big[60s]))', 60, 1200, 10));

SELECT '-- the split by primary key ranges can be switched off by the setting';
SELECT countIf(explain LIKE '%FilterSortedStreamByRange%') AS ranges,
       countIf(explain LIKE '%FinishAggregatingInOrderTransform%') > 0 AS merging
FROM (EXPLAIN PIPELINE SELECT * FROM prometheusQueryRange('prometheus_big', 'sum(rate(big[60s]))', 60, 1200, 10)
      SETTINGS aggregation_in_order_split_by_primary_key_ranges = 0);

-- The result with the split is materialized once, so each query reads the samples table once.
SELECT '-- the results do not depend on the split';
DROP TABLE IF EXISTS prometheus_big_result;
CREATE TABLE prometheus_big_result ENGINE = Memory AS
SELECT tags, samples FROM prometheusQueryRange('prometheus_big', 'sum by (inst) (rate(big[60s]))', 60, 1200, 100);
SELECT (SELECT groupArray((tags, samples)) FROM (SELECT tags, samples FROM prometheus_big_result ORDER BY tags))
     = (SELECT groupArray((tags, samples)) FROM (SELECT * FROM prometheusQueryRange('prometheus_big', 'sum by (inst) (rate(big[60s]))', 60, 1200, 100) ORDER BY tags) SETTINGS aggregation_in_order_split_by_primary_key_ranges = 0);

DROP TABLE prometheus_big_result;
DROP TABLE prometheus_big;
