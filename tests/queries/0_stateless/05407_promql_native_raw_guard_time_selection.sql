-- Tags: no-fasttest, no-parallel-replicas, no-replicated-database
-- Random settings limits: serialize_query_plan=(0, 0); max_threads=(2, 2); max_block_size=(8192, 8192)
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: this test checks local raw native input and PREWHERE.
-- Tag no-replicated-database: deferred drops of `TimeSeries` inner tables need a local database.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';
SET serialize_query_plan = 0;
SET max_threads = 2;
SET max_block_size = 8192;
SET min_promql_native_query_range_points = 0;
SET enable_promql_compensated_sum = 0;
SET enable_promql_native_parallel_processing = 0;
SET enable_promql_native_storage_fusion = 0;
SET max_promql_native_rate_samples_per_series = 4;
SET use_index_for_in_with_subqueries = 0;
SET short_circuit_function_evaluation = 'disable';

DROP TABLE IF EXISTS promql_native_raw_guard_time_selection;

CREATE TABLE promql_native_raw_guard_time_selection ENGINE = TimeSeries
SETTINGS version = 11, samples_bucket_step_seconds = 60, samples_index_granularity = 8192,
    samples_index_granularity_bytes = 0, recent_samples_ttl_seconds = 0
SAMPLES INNER ENGINE = AggregatingMergeTree
SETTINGS min_bytes_for_wide_part = 0;

-- One insert produces two rows of the same ID in one part and one fixed granule.
-- The selector for evaluation times 70..80 is [50.001, 80]. Both buckets pass
-- the coarse bucket bounds, but the oversized row fails `max_time >= 50.001`.
INSERT INTO promql_native_raw_guard_time_selection (metric_name, tags, samples) VALUES
    ('m', map('instance', 'selected'),
        [(toDateTime64(0, 3), 100.),
         (toDateTime64(10, 3), 110.),
         (toDateTime64(20, 3), 120.),
         (toDateTime64(30, 3), 130.),
         (toDateTime64(40, 3), 140.),
         (toDateTime64(60, 3), 100.),
         (toDateTime64(70, 3), 110.),
         (toDateTime64(80, 3), 120.),
         (toDateTime64(90, 3), 130.)]);

SELECT 'physical_rows', count() = 2 AND uniqExact(id) = 1 AND uniqExact(bucket) = 2
    AND uniqExact(_part) = 1 AND arraySort(groupArray(length(samples))) = [4, 5]
FROM timeSeriesSamples(promql_native_raw_guard_time_selection);
SELECT 'time_selection',
    countIf(bucket >= toDateTime64(0, 3) AND bucket <= toDateTime64(80, 3)) = 2
    AND countIf(length(samples) = 5 AND max_time < toDateTime64(50.001, 3)) = 1
    AND countIf(length(samples) = 4 AND max_time >= toDateTime64(50.001, 3)
        AND min_time <= toDateTime64(80, 3)) = 1
FROM timeSeriesSamples(promql_native_raw_guard_time_selection);

-- Prove the excluded row cannot be removed as a separate primary-key granule.
-- Inspect only the primary-key stage: partition pruning also reports granules.
-- This direct samples-table plan excludes the tags-table subquery's index stages.
SELECT 'one_granule', extract(arrayStringConcat(groupArray(trimLeft(explain)), '|'),
    'PrimaryKey\\|.*?Granules: ([0-9]+/[0-9]+)(?:\\||$)') = '1/1'
FROM
(
    EXPLAIN indexes = 1
    SELECT id, bucket, samples FROM timeSeriesSamples(promql_native_raw_guard_time_selection)
    PREWHERE bucket >= toDateTime64(0, 3) AND bucket <= toDateTime64(80, 3)
        AND max_time >= toDateTime64(50.001, 3) AND min_time <= toDateTime64(80, 3)
    SETTINGS enable_multiple_prewhere_read_steps = 0, allow_reorder_prewhere_conditions = 0
);

CREATE TEMPORARY TABLE raw_guard_sql AS
SELECT tags, samples FROM prometheusQueryRange(
    promql_native_raw_guard_time_selection, 'rate(m{instance="selected"}[20s])', 70, 80, 10)
SETTINGS enable_promql_native_plan = 0, enable_promql_native_raw_samples = 0;

SELECT 'sql_shape', count() = 1 AND sum(length(samples)) = 2
    AND countIf(tags = [('instance', 'selected')]
        AND arrayMap(sample -> toUnixTimestamp64Milli(sample.1), samples) = [70000, 80000]) = 1
    AND sum(arrayCount(sample -> isFinite(sample.2) AND abs(sample.2 - 1.) < 1e-10, samples)) = 2
FROM raw_guard_sql;

SET enable_promql_native_plan = 1;
SET enable_promql_native_raw_samples = 1;

-- Match the native step's immediate raw input, not another reader or an SQL fallback.
SELECT 'raw_native_input', match(arrayStringConcat(groupArray(replaceRegexpOne(explain, '^ +', '')), '\n'),
    'PromQLRangeRate\nInput headers: #0\nid [^\n]+\nbucket [^\n]+\nsamples Array\\(Tuple\\(')
FROM
(
    EXPLAIN PLAN header = 0, input_headers = 1, description = 0, pretty = 0
    SELECT * FROM prometheusQueryRange(
        promql_native_raw_guard_time_selection, 'rate(m{instance="selected"}[20s])', 70, 80, 10)
    SETTINGS enable_multiple_prewhere_read_steps = 0, allow_reorder_prewhere_conditions = 0
);

-- Keep eager evaluation in all four cases. The single-step case must not run
-- `throwIf` on the oversized, same-ID row excluded only by its sample time bounds.
CREATE TEMPORARY TABLE raw_guard_native AS
SELECT 'single' AS policy, tags, samples FROM prometheusQueryRange(
    promql_native_raw_guard_time_selection, 'rate(m{instance="selected"}[20s])', 70, 80, 10)
SETTINGS enable_multiple_prewhere_read_steps = 0, allow_reorder_prewhere_conditions = 0;
INSERT INTO raw_guard_native
SELECT 'single_reordered' AS policy, tags, samples FROM prometheusQueryRange(
    promql_native_raw_guard_time_selection, 'rate(m{instance="selected"}[20s])', 70, 80, 10)
SETTINGS enable_multiple_prewhere_read_steps = 0, allow_reorder_prewhere_conditions = 1;
INSERT INTO raw_guard_native
SELECT 'multiple' AS policy, tags, samples FROM prometheusQueryRange(
    promql_native_raw_guard_time_selection, 'rate(m{instance="selected"}[20s])', 70, 80, 10)
SETTINGS enable_multiple_prewhere_read_steps = 1, allow_reorder_prewhere_conditions = 0;
INSERT INTO raw_guard_native
SELECT 'multiple_reordered' AS policy, tags, samples FROM prometheusQueryRange(
    promql_native_raw_guard_time_selection, 'rate(m{instance="selected"}[20s])', 70, 80, 10)
SETTINGS enable_multiple_prewhere_read_steps = 1, allow_reorder_prewhere_conditions = 1;

SELECT policy, count() = 1 AND sum(length(samples)) = 2
FROM raw_guard_native GROUP BY policy ORDER BY policy;

CREATE TEMPORARY TABLE raw_guard_expected AS
SELECT policy, tags, samples FROM raw_guard_sql
CROSS JOIN (SELECT arrayJoin(['single', 'single_reordered', 'multiple', 'multiple_reordered']) AS policy) AS policies;

-- Row order may differ; labels, evaluation times, values and multiplicity may not.
SELECT 'native_minus_sql', count() = 0 FROM
    (SELECT * FROM raw_guard_native EXCEPT ALL SELECT * FROM raw_guard_expected);
SELECT 'sql_minus_native', count() = 0 FROM
    (SELECT * FROM raw_guard_expected EXCEPT ALL SELECT * FROM raw_guard_native);

-- The same oversized physical row must still throw when it overlaps the selector.
SELECT count() FROM prometheusQueryRange(
    promql_native_raw_guard_time_selection, 'rate(m{instance="selected"}[20s])', 20, 30, 10)
SETTINGS enable_multiple_prewhere_read_steps = 0, allow_reorder_prewhere_conditions = 0; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

DROP TABLE raw_guard_sql;
DROP TABLE raw_guard_native;
DROP TABLE raw_guard_expected;
DROP TABLE promql_native_raw_guard_time_selection SYNC;
