-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- `timeSeriesSelector` reads the samples table of a table with buckets of samples with `id IN <tags subquery>` only:
-- the id set prunes the granules in the primary-key index analysis and filters the rows. The tables of earlier versions
-- can replace the set with a primary-key range of the whole metric in the index analysis after a probe of the tags table,
-- see 04836_time_series_v10_selector_whole_metric_pk_range.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;

-- Two parts, one metric each: `max_bytes_to_merge_at_max_space_in_pool = 1` keeps them apart.
CREATE TABLE ts ENGINE = TimeSeries TAGS INNER COLUMNS (id Tuple(UInt64, UUID))
SAMPLES INNER ENGINE = AggregatingMergeTree ORDER BY (id, bucket) SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('foo', map('env', 'prod'), [(toDateTime64(100, 3), 1.), (toDateTime64(200, 3), 2.)]),
    ('foo', map('env', 'dev'), [(toDateTime64(150, 3), 10.)]);
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('bar', map('env', 'dev'), [(toDateTime64(100, 3), 100.)]);

SELECT '-- a whole-metric selector: the id set is the only condition on id, the index analysis uses it and prunes the part of the other metric';
SELECT sample.1 AS timestamp, sample.2 AS value FROM (SELECT arrayJoin(samples) AS sample FROM timeSeriesSelector(ts, 'foo', 0, 1000, 'samples')) ORDER BY value, timestamp;
SELECT countIf(explain ILIKE '%Condition:%' AND explain ILIKE '%element set%') > 0 AS id_set_in_index_analysis,
       countIf(explain ILIKE '%ffffffff-ffff-ffff-ffff-ffffffffffff%') = 0 AS no_id_range,
       countIf(explain ILIKE '%filter column:%' AND explain ILIKE '%IN subquery%') > 0 AS id_set_in_filter,
       countIf(explain LIKE '%Parts: 1/2%') > 0 AS prunes_the_other_metrics_part
FROM (EXPLAIN indexes = 1 SELECT sum(length(samples)) FROM timeSeriesSelector(ts, 'foo', 0, 1000, 'samples'));

SELECT '-- a partial selector: the same';
SELECT sample.1 AS timestamp, sample.2 AS value FROM (SELECT arrayJoin(samples) AS sample FROM timeSeriesSelector(ts, 'foo{env="prod"}', 0, 1000, 'samples')) ORDER BY value, timestamp;
SELECT countIf(explain ILIKE '%Condition:%' AND explain ILIKE '%element set%') > 0 AS id_set_in_index_analysis,
       countIf(explain ILIKE '%ffffffff-ffff-ffff-ffff-ffffffffffff%') = 0 AS no_id_range,
       countIf(explain ILIKE '%filter column:%' AND explain ILIKE '%IN subquery%') > 0 AS id_set_in_filter,
       countIf(explain LIKE '%Parts: 1/2%') > 0 AS prunes_the_other_metrics_part
FROM (EXPLAIN indexes = 1 SELECT sum(length(samples)) FROM timeSeriesSelector(ts, 'foo{env="prod"}', 0, 1000, 'samples'));

DROP TABLE ts;
