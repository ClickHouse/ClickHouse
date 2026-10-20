-- Tags: no-fasttest, no-parallel-replicas, no-replicated-database
-- Random settings limits: serialize_query_plan=(0, 0); max_threads=(4, 4); max_block_size=(1, 1); optimize_read_in_order=(1, 1); max_threads_min_free_memory_per_thread=(0, 0); max_streams_to_max_threads_ratio=(1, 1); max_streams_for_merge_tree_reading=(0, 0); allow_asynchronous_read_from_io_pool_for_merge_tree=(0, 0)
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: this test compares local SQL and native plans.
-- Tag no-replicated-database: deferred drops of `TimeSeries` inner tables need a local database.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';
SET serialize_query_plan = 0;
SET max_threads = 4;
SET max_block_size = 1;
SET optimize_read_in_order = 1;
SET max_threads_min_free_memory_per_thread = 0;
SET max_streams_to_max_threads_ratio = 1;
SET max_streams_for_merge_tree_reading = 0;
SET allow_asynchronous_read_from_io_pool_for_merge_tree = 0;
SET min_promql_native_query_range_points = 0;
SET max_promql_native_rate_samples_per_series = 100;
SET max_promql_native_parallel_lanes = 0;
SET enable_promql_compensated_sum = 0;
SET enable_promql_native_storage_fusion = 0;
SET enable_promql_native_raw_samples = 0;

DROP TABLE IF EXISTS promql_native_range_sum_by_stale_markers;

CREATE TABLE promql_native_range_sum_by_stale_markers ENGINE = TimeSeries
SETTINGS version = 11, samples_index_granularity = 1, recent_samples_ttl_seconds = 0;

-- The two `stale` series have distinct full label sets but one output group.
-- Only the exact Prometheus stale payload is omitted; the ordinary NaN is data.
INSERT INTO promql_native_range_sum_by_stale_markers (metric_name, tags, samples) VALUES
    ('m', map('instance', 'stale', 'source', 'with_marker'),
        [(toDateTime64(81, 3), 0.),
         (toDateTime64(91, 3), 10.),
         (toDateTime64(95, 3), reinterpretAsFloat64(toUInt64(0x7FF0000000000002))),
         (toDateTime64(100, 3), 20.)]),
    ('m', map('instance', 'stale', 'source', 'finite'),
        [(toDateTime64(81, 3), 0.),
         (toDateTime64(91, 3), 10.),
         (toDateTime64(100, 3), 20.)]),
    ('m', map('instance', 'trailing'),
        [(toDateTime64(81, 3), 0.),
         (toDateTime64(91, 3), 10.),
         (toDateTime64(100, 3), reinterpretAsFloat64(toUInt64(0x7FF0000000000002)))]),
    ('m', map('instance', 'all_stale'),
        [(toDateTime64(81, 3), reinterpretAsFloat64(toUInt64(0x7FF0000000000002))),
         (toDateTime64(91, 3), reinterpretAsFloat64(toUInt64(0x7FF0000000000002))),
         (toDateTime64(100, 3), reinterpretAsFloat64(toUInt64(0x7FF0000000000002)))]),
    ('m', map('instance', 'ordinary'),
        [(toDateTime64(81, 3), 0.),
         (toDateTime64(91, 3), 10.),
         (toDateTime64(100, 3), reinterpretAsFloat64(toUInt64(0x7FF8000000000001)))]);

SELECT 'physical_markers', count() = 5
    AND sum(arrayCount(sample -> reinterpretAsUInt64(sample.2) = toUInt64(0x7FF0000000000002), samples)) = 5
    AND sum(arrayCount(sample -> reinterpretAsUInt64(sample.2) = toUInt64(0x7FF8000000000001), samples)) = 1
FROM timeSeriesSamples(promql_native_range_sum_by_stale_markers);

-- Preserve the NaN classification while making exact multiset equality independent
-- of NaN comparison and payload propagation. Finite values and timestamps stay exact.
CREATE TEMPORARY TABLE stale_sum_sql AS
SELECT tags, arrayMap(sample -> tuple(toUnixTimestamp64Milli(sample.1), isNaN(sample.2),
    if(isNaN(sample.2), 0., sample.2)), samples) AS samples
FROM prometheusQueryRange(
    promql_native_range_sum_by_stale_markers, 'sum by(instance)(rate(m[20s]))', 100, 100, 10)
SETTINGS enable_promql_native_plan = 0, enable_promql_native_parallel_processing = 0;

CREATE TEMPORARY TABLE stale_sum_serial AS
SELECT tags, arrayMap(sample -> tuple(toUnixTimestamp64Milli(sample.1), isNaN(sample.2),
    if(isNaN(sample.2), 0., sample.2)), samples) AS samples
FROM prometheusQueryRange(
    promql_native_range_sum_by_stale_markers, 'sum by(instance)(rate(m[20s]))', 100, 100, 10)
SETTINGS enable_promql_native_plan = 1, enable_promql_native_parallel_processing = 0;

-- The parallel setting and the `PromQLRangeSumBy` plan check prove native-path
-- eligibility, not execution of the multi-stream merge branch.
CREATE TEMPORARY TABLE stale_sum_parallel AS
SELECT tags, arrayMap(sample -> tuple(toUnixTimestamp64Milli(sample.1), isNaN(sample.2),
    if(isNaN(sample.2), 0., sample.2)), samples) AS samples
FROM prometheusQueryRange(
    promql_native_range_sum_by_stale_markers, 'sum by(instance)(rate(m[20s]))', 100, 100, 10)
SETTINGS enable_promql_native_plan = 1, enable_promql_native_parallel_processing = 1;

-- The trailing-stale result must be finite and nonempty; SQL is its extrapolation oracle.
-- The all-stale series must not create an output group.
SELECT policy,
    count() = 3 AND sum(length(samples)) = 3,
    countIf(tags = [('instance', 'stale')] AND length(samples) = 1
        AND samples[1].1 = 100000 AND samples[1].2 = 0 AND abs(samples[1].3 - 2.) < 1e-10) = 1,
    countIf(tags = [('instance', 'trailing')] AND length(samples) = 1
        AND samples[1].1 = 100000 AND samples[1].2 = 0 AND isFinite(samples[1].3) AND samples[1].3 > 0.) = 1,
    countIf(tags = [('instance', 'ordinary')] AND length(samples) = 1
        AND samples[1].1 = 100000 AND samples[1].2 = 1) = 1,
    countIf(tags = [('instance', 'all_stale')]) = 0
FROM
(
    SELECT 'sql' AS policy, * FROM stale_sum_sql
    UNION ALL SELECT 'serial' AS policy, * FROM stale_sum_serial
    UNION ALL SELECT 'parallel' AS policy, * FROM stale_sum_parallel
)
GROUP BY policy ORDER BY policy;

-- Row order may differ; labels, samples, timestamps and multiplicity may not.
SELECT 'sql_minus_serial', count() = 0 FROM
    (SELECT * FROM stale_sum_sql EXCEPT ALL SELECT * FROM stale_sum_serial);
SELECT 'serial_minus_sql', count() = 0 FROM
    (SELECT * FROM stale_sum_serial EXCEPT ALL SELECT * FROM stale_sum_sql);
SELECT 'sql_minus_parallel', count() = 0 FROM
    (SELECT * FROM stale_sum_sql EXCEPT ALL SELECT * FROM stale_sum_parallel);
SELECT 'parallel_minus_sql', count() = 0 FROM
    (SELECT * FROM stale_sum_parallel EXCEPT ALL SELECT * FROM stale_sum_sql);

SELECT 'serial_native_step', countIf(explain LIKE '%PromQLRangeSumBy%') > 0
FROM
(
    EXPLAIN PIPELINE
    SELECT * FROM prometheusQueryRange(
        promql_native_range_sum_by_stale_markers, 'sum by(instance)(rate(m[20s]))', 100, 100, 10)
    SETTINGS enable_promql_native_plan = 1, enable_promql_native_parallel_processing = 0
);
SELECT 'parallel_native_step', countIf(explain LIKE '%PromQLRangeSumBy%') > 0
FROM
(
    EXPLAIN PIPELINE
    SELECT * FROM prometheusQueryRange(
        promql_native_range_sum_by_stale_markers, 'sum by(instance)(rate(m[20s]))', 100, 100, 10)
    SETTINGS enable_promql_native_plan = 1, enable_promql_native_parallel_processing = 1
);

-- The four physical samples of `with_marker` exceed the limit even though only
-- three remain after stale filtering. Both native policies must retain this cap.
SELECT count() FROM prometheusQueryRange(
    promql_native_range_sum_by_stale_markers, 'sum by(instance)(rate(m{instance="stale"}[20s]))', 100, 100, 10)
SETTINGS enable_promql_native_plan = 1, enable_promql_native_parallel_processing = 0,
    max_promql_native_rate_samples_per_series = 3; -- { serverError TOO_MANY_ROWS_OR_BYTES }
SELECT count() FROM prometheusQueryRange(
    promql_native_range_sum_by_stale_markers, 'sum by(instance)(rate(m{instance="stale"}[20s]))', 100, 100, 10)
SETTINGS enable_promql_native_plan = 1, enable_promql_native_parallel_processing = 1,
    max_promql_native_rate_samples_per_series = 3; -- { serverError TOO_MANY_ROWS_OR_BYTES }

DROP TABLE stale_sum_sql;
DROP TABLE stale_sum_serial;
DROP TABLE stale_sum_parallel;
DROP TABLE promql_native_range_sum_by_stale_markers SYNC;

-- Four complete IDs share one projected group. Single-row granules provide
-- whole-ID split boundaries; require `PromQLPartialGroupMerge`, not only the flag.
DROP TABLE IF EXISTS promql_native_range_sum_by_stale_lanes;

CREATE TABLE promql_native_range_sum_by_stale_lanes ENGINE = TimeSeries
SETTINGS version = 11, samples_index_granularity = 1, recent_samples_ttl_seconds = 0;

INSERT INTO promql_native_range_sum_by_stale_lanes (metric_name, tags, samples) VALUES
    ('m', map('instance', 'shared', 'source', 'a'),
        [(toDateTime64(81, 3), 0.),
         (toDateTime64(91, 3), 10.),
         (toDateTime64(95, 3), reinterpretAsFloat64(toUInt64(0x7FF0000000000002))),
         (toDateTime64(100, 3), 20.)]),
    ('m', map('instance', 'shared', 'source', 'b'),
        [(toDateTime64(81, 3), 0.),
         (toDateTime64(91, 3), 10.),
         (toDateTime64(95, 3), reinterpretAsFloat64(toUInt64(0x7FF0000000000002))),
         (toDateTime64(100, 3), 20.)]),
    ('m', map('instance', 'shared', 'source', 'c'),
        [(toDateTime64(81, 3), 0.),
         (toDateTime64(91, 3), 10.),
         (toDateTime64(95, 3), reinterpretAsFloat64(toUInt64(0x7FF0000000000002))),
         (toDateTime64(100, 3), 20.)]),
    ('m', map('instance', 'shared', 'source', 'd'),
        [(toDateTime64(81, 3), 0.),
         (toDateTime64(91, 3), 10.),
         (toDateTime64(95, 3), reinterpretAsFloat64(toUInt64(0x7FF0000000000002))),
         (toDateTime64(100, 3), 20.)]);

SELECT 'parallel_physical_ids', count() = 4 AND uniqExact(id) = 4
    AND sum(arrayCount(sample -> reinterpretAsUInt64(sample.2) = toUInt64(0x7FF0000000000002), samples)) = 4
FROM timeSeriesSamples(promql_native_range_sum_by_stale_lanes);

CREATE TEMPORARY TABLE stale_lanes_sql AS
SELECT tags, samples FROM prometheusQueryRange(
    promql_native_range_sum_by_stale_lanes, 'sum by(instance)(rate(m[20s]))', 100, 100, 10)
SETTINGS enable_promql_native_plan = 0, enable_promql_native_parallel_processing = 0;

CREATE TEMPORARY TABLE stale_lanes_native AS
SELECT tags, samples FROM prometheusQueryRange(
    promql_native_range_sum_by_stale_lanes, 'sum by(instance)(rate(m[20s]))', 100, 100, 10)
SETTINGS enable_promql_native_plan = 1, enable_promql_native_parallel_processing = 1,
    max_promql_native_parallel_lanes = 2;

SELECT 'parallel_partial_merge', countIf(explain LIKE '%PromQLPartialGroupMerge%') = 1
FROM
(
    EXPLAIN PIPELINE
    SELECT * FROM prometheusQueryRange(
        promql_native_range_sum_by_stale_lanes, 'sum by(instance)(rate(m[20s]))', 100, 100, 10)
    SETTINGS enable_promql_native_plan = 1, enable_promql_native_parallel_processing = 1,
        max_promql_native_parallel_lanes = 2
);

SELECT 'parallel_shared_group', count() = 1 AND sum(length(samples)) = 1
    AND countIf(tags = [('instance', 'shared')] AND length(samples) = 1
        AND samples[1].1 = toDateTime64(100, 3) AND isFinite(samples[1].2) AND samples[1].2 > 0.) = 1
FROM stale_lanes_native;

SELECT 'lanes_sql_minus_native', count() = 0 FROM
    (SELECT * FROM stale_lanes_sql EXCEPT ALL SELECT * FROM stale_lanes_native);
SELECT 'lanes_native_minus_sql', count() = 0 FROM
    (SELECT * FROM stale_lanes_native EXCEPT ALL SELECT * FROM stale_lanes_sql);

DROP TABLE stale_lanes_sql;
DROP TABLE stale_lanes_native;
DROP TABLE promql_native_range_sum_by_stale_lanes SYNC;
