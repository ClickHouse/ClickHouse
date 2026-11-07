SET enable_time_series_table = 1;
SET session_timezone = 'UTC';
-- The hints are checked as the sink wrote them, before any merge by `timeSeriesGroupArrayHistograms` (05494 covers the merges).
SET optimize_on_insert = 0;

DROP TABLE IF EXISTS ts_reset_detection;
DROP TABLE IF EXISTS histogram_reset_cases;

-- Each row is one input series. Run every case with both integer and float histograms.
CREATE TABLE histogram_reset_cases
(
    name String,
    timestamps Array(DateTime64(3)) DEFAULT [toDateTime64('2026-01-01 00:01:40', 3), toDateTime64('2026-01-01 00:01:41', 3)],
    hints Array(UInt8) DEFAULT [0, 2],
    schemas Array(Int8) DEFAULT [0, 0],
    thresholds Array(Float64) DEFAULT [0, 0],
    sums Array(Float64) DEFAULT [1, 1],
    zeros Array(Int64) DEFAULT [0, 0],
    positive Array(Array(Tuple(index Int32, count Int64))) DEFAULT [[], []],
    negative Array(Array(Tuple(index Int32, count Int64))) DEFAULT [[], []],
    bounds Array(Array(Float64)) DEFAULT [[], []],
    expected Array(UInt8)
) ENGINE = Memory;

INSERT INTO histogram_reset_cases (name, zeros, expected) VALUES
    ('total_count_decrease', [10, 9], [0, 1]);
-- Total count increases, but a zero, positive, or negative bucket decreases.
INSERT INTO histogram_reset_cases (name, zeros, positive, expected) VALUES
    ('zero_bucket_decrease', [2, 1], [[(0, 1)], [(0, 3)]], [0, 1]);
INSERT INTO histogram_reset_cases (name, positive, expected) VALUES
    ('positive_bucket_decrease', [[(-1, 7), (2, 3)], [(-1, 6), (2, 5)]], [0, 1]),
    ('positive_bucket_disappears', [[(-1, 1), (0, 2), (1, 3)], [(-1, 2), (1, 5)]], [0, 1]),
    ('last_positive_bucket_disappears', [[(0, 7), (1, 3)], [(0, 11)]], [0, 1]);
INSERT INTO histogram_reset_cases (name, negative, expected) VALUES
    ('negative_bucket_decrease', [[(-1, 7), (2, 3)], [(-1, 6), (2, 5)]], [0, 1]),
    ('negative_bucket_disappears', [[(0, 7), (1, 3)], [(0, 11)]], [0, 1]);
INSERT INTO histogram_reset_cases (name, zeros, positive, expected) VALUES
    ('all_positive_buckets_disappear', [0, 3], [[(0, 2)], []], [0, 1]);
INSERT INTO histogram_reset_cases (name, zeros, negative, expected) VALUES
    ('all_negative_buckets_disappear', [0, 3], [[(0, 2)], []], [0, 1]);

INSERT INTO histogram_reset_cases (name, schemas, positive, bounds, expected) VALUES
    ('custom_bounds_change', [-53, -53], [[(0, 1)], [(0, 1)]], [[1], [2]], [0, 1]),
    ('custom_bounds_length_change', [-53, -53], [[(0, 1)], [(0, 1)]], [[1], [1, 2]], [0, 1]),
    -- The bounds are compared by value, as Prometheus does: -0.0 and 0.0 are the same bound.
    ('custom_bounds_signed_zero_same', [-53, -53], [[(0, 1)], [(0, 1)]], [[-0.0], [0.0]], [0, 2]);
INSERT INTO histogram_reset_cases (name, hints, zeros, expected) VALUES
    ('explicit_reset_without_decrease', [0, 1], [1, 2], [0, 1]);
INSERT INTO histogram_reset_cases (name, hints, schemas, zeros, expected) VALUES
    ('explicit_reset_before_schema_change', [0, 1], [0, 1], [1, 2], [0, 1]);

-- Schema and threshold changes are unknown unless the count decrease or an explicit hint already established a reset.
INSERT INTO histogram_reset_cases (name, schemas, zeros, expected) VALUES
    ('schema_increase', [0, 1], [1, 2], [0, 0]),
    ('schema_decrease', [1, 0], [1, 2], [0, 0]),
    ('count_decrease_before_schema_change', [0, 1], [2, 1], [0, 1]);
INSERT INTO histogram_reset_cases (name, schemas, positive, bounds, expected) VALUES
    ('exponential_to_custom', [0, -53], [[(0, 1)], [(0, 1)]], [[], [1]], [0, 0]),
    ('custom_to_exponential', [-53, 0], [[(0, 1)], [(0, 1)]], [[1], []], [0, 0]);
INSERT INTO histogram_reset_cases (name, thresholds, zeros, expected) VALUES
    ('zero_threshold_increase', [0.001, 0.01], [1, 2], [0, 0]),
    ('zero_threshold_decrease', [0.01, 0.001], [1, 2], [0, 0]);

-- These changes are not resets. In particular, negative observations can decrease the sum.
INSERT INTO histogram_reset_cases (name, hints, zeros, expected) VALUES
    ('unknown_becomes_no_reset', [0, 0], [1, 2], [0, 2]);
INSERT INTO histogram_reset_cases (name, sums, zeros, expected) VALUES
    ('sum_decrease', [10, -10], [1, 2], [0, 2]);
INSERT INTO histogram_reset_cases (name, positive, expected) VALUES
    ('empty_positive_bucket_disappears', [[(0, 1), (1, 0)], [(0, 1)]], [0, 2]),
    ('new_positive_buckets', [[(1, 1)], [(0, 1), (1, 1), (3, 1)]], [0, 2]);
INSERT INTO histogram_reset_cases (name, negative, expected) VALUES
    ('empty_negative_bucket_disappears', [[(0, 1), (1, 0)], [(0, 1)]], [0, 2]);
INSERT INTO histogram_reset_cases (name, hints, zeros, expected) VALUES
    ('gauge_decrease', [3, 3], [10, 1], [3, 3]),
    ('gauge_to_counter', [3, 2], [10, 1], [3, 0]),
    ('counter_to_gauge', [0, 3], [10, 1], [0, 3]);

-- Stale markers have unknown hints, including the first sample and explicit reset or gauge hints.
-- Only the exact NaN payload marks staleness; ordinary NaNs still allow reset detection.
INSERT INTO histogram_reset_cases (name, sums, zeros, expected)
SELECT 'stale_current', [1., reinterpretAsFloat64(toUInt64(9218868437227405314))], [10, 0], [0, 0]
UNION ALL
SELECT 'stale_previous', [reinterpretAsFloat64(toUInt64(9218868437227405314)), 1.], [10, 0], [0, 0]
UNION ALL
SELECT 'ordinary_nan', [nan, nan], [2, 1], [0, 1];
INSERT INTO histogram_reset_cases (name, hints, sums, zeros, expected)
SELECT 'explicit_reset_on_stale', [0, 1], [1., reinterpretAsFloat64(toUInt64(9218868437227405314))], [10, 0], [0, 0]
UNION ALL
SELECT 'gauge_on_stale', [3, 3], [1., reinterpretAsFloat64(toUInt64(9218868437227405314))], [10, 0], [3, 0]
UNION ALL
SELECT 'first_stale_no_reset', [2, 2], [reinterpretAsFloat64(toUInt64(9218868437227405314)), 1.], [10, 0], [0, 0]
UNION ALL
SELECT 'first_stale_reset', [1, 2], [reinterpretAsFloat64(toUInt64(9218868437227405314)), 1.], [10, 0], [0, 0]
UNION ALL
SELECT 'first_stale_gauge', [3, 2], [reinterpretAsFloat64(toUInt64(9218868437227405314)), 1.], [10, 0], [0, 0]
UNION ALL
SELECT 'consecutive_stale', [1, 2], arrayWithConstant(2, reinterpretAsFloat64(toUInt64(9218868437227405314))), [10, 0], [0, 0]
UNION ALL
SELECT 'explicit_reset_after_stale', [0, 1], [reinterpretAsFloat64(toUInt64(9218868437227405314)), 1.], [10, 0], [0, 1]
UNION ALL
SELECT 'gauge_after_stale', [0, 3], [reinterpretAsFloat64(toUInt64(9218868437227405314)), 1.], [10, 0], [0, 3];

-- Infer against the final sorted neighbors; a moved explicit reset is kept.
INSERT INTO histogram_reset_cases (name, timestamps, hints, zeros, expected) VALUES
    ('unsorted_decrease', ['2026-01-01 00:01:41', '2026-01-01 00:01:40'], [2, 2], [1, 2], [2, 1]),
    ('moved_explicit_reset', ['2026-01-01 00:01:41', '2026-01-01 00:01:40'], [1, 2], [2, 1], [2, 1]),
    ('bucket_boundary', ['2026-01-01 00:01:59', '2026-01-01 00:02:00'], [0, 2], [2, 1], [0, 1]);

-- An explicit reset is kept whether or not its original predecessor survived the deduplication of equal timestamps (the sample with
-- the greatest count wins): a reset can't always be told from the data, and the counts don't decrease in either case.
INSERT INTO histogram_reset_cases (name, timestamps, hints, schemas, thresholds, sums, zeros, positive, negative, bounds, expected) VALUES
    ('explicit_reset_after_lost_duplicate', ['2026-01-01 00:01:40', '2026-01-01 00:01:40', '2026-01-01 00:01:41'],
     [0, 0, 1], [0, 0, 0], [0, 0, 0], [1, 1, 1], [5, 1, 6], [[], [], []], [[], [], []], [[], [], []], [0, 1]),
    ('explicit_reset_after_winning_duplicate', ['2026-01-01 00:01:40', '2026-01-01 00:01:40', '2026-01-01 00:01:41'],
     [0, 0, 1], [0, 0, 0], [0, 0, 0], [1, 1, 1], [1, 5, 6], [[], [], []], [[], [], []], [[], [], []], [0, 1]);

-- The first sample of a series keeps an explicit reset hint.
INSERT INTO histogram_reset_cases (name, hints, zeros, expected) VALUES
    ('first_sample_explicit_reset', [1, 2], [1, 2], [1, 2]);

-- A ten-year TTL keeps the fixed timestamps alive. Different bucket sizes exercise both output writers.
CREATE TABLE ts_reset_detection ENGINE = TimeSeries
SETTINGS samples_bucket_step_seconds = 60, recent_samples_bucket_step_seconds = 30, recent_samples_ttl_seconds = 315360000;
INSERT INTO ts_reset_detection
    (metric_name, tags, histograms.timestamp, histograms.flags, histograms.counter_reset_hint, histograms.schema,
     histograms.zero_threshold, histograms.sum, histograms.count_int, histograms.zero_count_int,
     histograms.positive_int, histograms.negative_int, histograms.count_float, histograms.zero_count_float,
     histograms.positive_float, histograms.negative_float, histograms.custom_values)
WITH arrayMap((z, p, n) -> z + arraySum(x -> x.2, p) + arraySum(x -> x.2, n), zeros, positive, negative) AS counts
SELECT 'reset_test', map('case', name, 'flavour', toString(is_float)), timestamps, arrayMap(t -> is_float, timestamps), hints, schemas,
       thresholds, sums,
       arrayMap(c -> if(is_float, 0, c), counts), arrayMap(c -> if(is_float, 0, c), zeros),
       arrayMap(p -> if(is_float, [], p), positive), arrayMap(n -> if(is_float, [], n), negative),
       arrayMap(c -> if(is_float, toFloat64(c), 0.), counts), arrayMap(c -> if(is_float, toFloat64(c), 0.), zeros),
       arrayMap(p -> if(is_float, arrayMap(x -> (x.1, toFloat64(x.2)), p), []), positive),
       arrayMap(n -> if(is_float, arrayMap(x -> (x.1, toFloat64(x.2)), n), []), negative), bounds
FROM histogram_reset_cases CROSS JOIN (SELECT arrayJoin([toUInt8(0), toUInt8(1)]) AS is_float) AS flavours;

-- Report individual cases on failure, including the actual and expected hints. Concatenate buckets without another aggregation
-- of histograms, which would itself clear boundary hints. Every case must be present in both tables and both flavours.
SELECT source, count() = (SELECT count() * 2 FROM histogram_reset_cases),
       arraySort(groupArrayIf((name, flavour, actual, expected), actual != expected)) AS mismatches
FROM
(
    SELECT s.source, c.name, t.tags['flavour'] AS flavour, c.expected,
           arrayFlatten(arrayMap(x -> x.2, arraySort(x -> x.1, groupArray((s.bucket, s.histograms.counter_reset_hint))))) AS actual
    FROM
    (
        SELECT 'samples' AS source, id, bucket, histograms FROM timeSeriesSamples(ts_reset_detection)
        UNION ALL
        SELECT 'recent' AS source, id, bucket, histograms FROM merge(currentDatabase(), '^\\.inner_id\\.recentsamples\\.')
    ) AS s
    INNER JOIN timeSeriesTags(ts_reset_detection) AS t ON s.id = t.id
    INNER JOIN histogram_reset_cases AS c ON t.tags['case'] = c.name
    GROUP BY s.source, c.name, flavour, c.expected
)
GROUP BY source ORDER BY source;

-- Integer reset checks must not round counts to Float64; fractional float decreases must also be detected.
INSERT INTO ts_reset_detection (metric_name, histograms.timestamp, histograms.count_int, histograms.zero_count_int, histograms.counter_reset_hint)
VALUES ('large_integer_count', ['2026-01-01 00:01:40', '2026-01-01 00:01:41'], [9007199254740993, 9007199254740992], [9007199254740993, 9007199254740992], [0, 2]);
INSERT INTO ts_reset_detection (metric_name, histograms.timestamp, histograms.flags, histograms.count_float, histograms.zero_count_float, histograms.counter_reset_hint)
VALUES ('fractional_float_count', ['2026-01-01 00:01:40', '2026-01-01 00:01:41'], [1, 1], [1.5, 1.25], [1.5, 1.25], [0, 2]);
-- A representation change starts a new history, even if the counts decrease.
INSERT INTO ts_reset_detection (metric_name, histograms.timestamp, histograms.flags, histograms.count_int, histograms.zero_count_int,
                               histograms.count_float, histograms.zero_count_float, histograms.counter_reset_hint)
VALUES ('integer_to_float', ['2026-01-01 00:01:40', '2026-01-01 00:01:41'], [0, 1], [10, 0], [10, 0], [0, 1], [0, 1], [0, 2]),
       ('float_to_integer', ['2026-01-01 00:01:40', '2026-01-01 00:01:41'], [1, 0], [0, 1], [0, 1], [10, 0], [10, 0], [0, 2]);
SELECT t.metric_name, s.histograms.counter_reset_hint
FROM timeSeriesSamples(ts_reset_detection) AS s INNER JOIN timeSeriesTags(ts_reset_detection) AS t ON s.id = t.id
WHERE t.metric_name != 'reset_test' ORDER BY t.metric_name;

DROP TABLE ts_reset_detection;
DROP TABLE histogram_reset_cases;
