-- Inserting native histogram samples into a TimeSeries table through the `histograms.*` outer columns (see TimeSeriesSink):
-- the sink sorts and deduplicates them per series, splits them into the buckets of the samples tables together with the float
-- samples (a row holds the float samples and the histogram samples of one series within one bucket), and writes the time ranges.
-- The stored rows are checked through the inner tables, so the checks depend on the sink only (reading the `histograms.*` outer
-- columns is covered by 05461_timeseries_histograms_outer_read).
-- Every case uses a metric name of its own, so the rows of a case never share a key with another part and the checks don't
-- depend on background merges. The columns of the `histograms` group which an INSERT doesn't mention get default values aligned
-- with the inserted arrays.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
DROP TABLE IF EXISTS ts_recent;

-- The buckets are 60 seconds long; the recent samples table is disabled because its TTL would drop the old samples.
CREATE TABLE ts ENGINE = TimeSeries SETTINGS samples_bucket_step_seconds = 60, recent_samples_ttl_seconds = 0;

SELECT '-- an integer histogram and a float histogram, stored as sent';
INSERT INTO ts (metric_name, tags, histograms.timestamp, histograms.flags, histograms.counter_reset_hint, histograms.schema, histograms.zero_threshold, histograms.sum,
                histograms.count_int, histograms.zero_count_int, histograms.positive_int, histograms.negative_int)
    VALUES ('int_hist', map('job', 'api'), [toDateTime64(100, 3)], [0], [2], [3], [0.001], [12.5], [10], [2], [[(-2, 3), (-1, 2), (1, 3)]], [[(0, 0)]]);
INSERT INTO ts (metric_name, tags, histograms.timestamp, histograms.flags, histograms.counter_reset_hint, histograms.schema, histograms.sum,
                histograms.count_float, histograms.zero_count_float, histograms.positive_float, histograms.negative_float)
    VALUES ('float_hist', map('job', 'api'), [toDateTime64(100, 3)], [1], [3], [0], [3.25], [5.75], [0.5], [[(0, 2.5), (1, 2.5)]], [[(1, 0.25)]]);

SELECT t.metric_name, toUnixTimestamp64Milli(h.timestamp) AS timestamp_ms, h.flags, h.counter_reset_hint, h.schema, h.zero_threshold, h.sum,
       h.count_int, h.zero_count_int, h.positive_int, h.negative_int, h.count_float, h.zero_count_float, h.positive_float, h.negative_float, h.custom_values,
       length(s.samples)
FROM timeSeriesSamples(ts) AS s ARRAY JOIN s.histograms AS h
JOIN timeSeriesTags(ts) AS t ON s.id = t.id
WHERE t.metric_name IN ('int_hist', 'float_hist')
ORDER BY t.metric_name;

SELECT '-- the float samples and the histograms of one series within one bucket make one row, its time range covers both kinds';
INSERT INTO ts (metric_name, tags, samples, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_int)
    VALUES ('mixed', map('job', 'api'), [(toDateTime64(130, 3), 1.5), (toDateTime64(150, 3), 2.5)],
            [toDateTime64(125, 3), toDateTime64(145, 3)], [1, 2], [0.5, 1.5], [[(0, 1)], [(0, 2)]]);

SELECT t.metric_name, s.bucket, s.min_time, s.max_time, s.samples, s.histograms.timestamp, s.histograms.count_int
FROM timeSeriesSamples(ts) AS s JOIN timeSeriesTags(ts) AS t ON s.id = t.id
WHERE t.metric_name = 'mixed'
ORDER BY s.bucket;

SELECT '-- the samples of several buckets make a row per bucket, each with the float samples and the histograms of its bucket';
INSERT INTO ts (metric_name, tags, samples, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_int)
    VALUES ('buckets', map('job', 'api'), [(toDateTime64(70, 3), 1.), (toDateTime64(130, 3), 2.), (toDateTime64(190, 3), 3.)],
            [toDateTime64(80, 3), toDateTime64(140, 3), toDateTime64(170, 3), toDateTime64(250, 3)], [1, 2, 3, 4], [0, 0, 0, 0],
            [[(0, 1)], [(0, 2)], [(0, 3)], [(0, 4)]]);

SELECT t.metric_name, s.bucket, s.min_time, s.max_time, s.samples, s.histograms.timestamp, s.histograms.count_int
FROM timeSeriesSamples(ts) AS s JOIN timeSeriesTags(ts) AS t ON s.id = t.id
WHERE t.metric_name = 'buckets'
ORDER BY s.bucket;

SELECT '-- unsorted histograms are sorted; of duplicates by timestamp the greatest count wins (counts compared exactly across flavours, a NaN count loses), then the greatest sum, then the other elements; never the input order';
-- Timestamp 20: equal counts, the greater sum comes first in the input. Timestamp 50: equal counts and sums, the pairs decide
-- (the count of the bucket 0: 2 beats 1), again against the input order. See TimeSeriesDuplicateTimestampRule.
INSERT INTO ts (metric_name, tags, histograms.timestamp, histograms.flags, histograms.count_int, histograms.count_float, histograms.sum, histograms.positive_int, histograms.positive_float)
    VALUES ('dups', map('job', 'api'),
            [toDateTime64(30, 3), toDateTime64(10, 3), toDateTime64(20, 3), toDateTime64(10, 3), toDateTime64(30, 3), toDateTime64(20, 3), toDateTime64(40, 3), toDateTime64(40, 3), toDateTime64(50, 3), toDateTime64(50, 3)],
            [0, 1, 0, 0, 0, 0, 0, 1, 0, 0],
            [5, 0, 7, 5, 4, 7, 1, 0, 3, 3],
            [0, 4.5, 0, 0, 0, 0, 0, nan, 0, 0],
            [1, 2, 6, 4, 5, 3, 7, 8, 9, 9],
            [[(0, 5)], [], [(0, 7)], [(0, 5)], [(0, 4)], [(0, 7)], [(0, 1)], [], [(0, 2), (1, 1)], [(0, 1), (1, 2)]],
            [[], [(0, 4.5)], [], [], [], [], [], [], [], []]);

SELECT t.metric_name, toUnixTimestamp64Milli(h.timestamp) AS timestamp_ms, h.flags, h.count_int, h.count_float, h.sum, h.positive_int, h.positive_float
FROM timeSeriesSamples(ts) AS s ARRAY JOIN s.histograms AS h
JOIN timeSeriesTags(ts) AS t ON s.id = t.id
WHERE t.metric_name = 'dups'
ORDER BY h.timestamp;

SELECT '-- the time range of a series covers its float samples and its histograms';
SELECT t.metric_name, min(r.min_time), max(r.max_time)
FROM timeSeriesTimeRanges(ts) AS r JOIN timeSeriesTags(ts) AS t ON r.id = t.id
GROUP BY t.metric_name
ORDER BY t.metric_name;

SELECT '-- errors: a histogram needs a series, the payload columns need the timestamps';
INSERT INTO ts (histograms.timestamp, histograms.count_int) VALUES ([toDateTime64(100, 3)], [0]); -- { serverError INCORRECT_DATA }
INSERT INTO ts (metric_name, tags, histograms.count_int) VALUES ('x', map('job', 'api'), [1]); -- { serverError ILLEGAL_COLUMN }
SELECT count() FROM timeSeriesTags(ts) WHERE metric_name = 'x';

SELECT '-- histograms.timestamp alone is a valid empty integer histogram: the other columns default to zeros and empty arrays aligned with the timestamps';
INSERT INTO ts (metric_name, tags, histograms.timestamp) VALUES ('timestamps_only', map('job', 'api'), [toDateTime64(100, 3), toDateTime64(160, 3)]);

SELECT t.metric_name, s.bucket, length(s.samples), s.histograms.timestamp, s.histograms.flags, s.histograms.count_int, s.histograms.positive_int
FROM timeSeriesSamples(ts) AS s JOIN timeSeriesTags(ts) AS t ON s.id = t.id
WHERE t.metric_name = 'timestamps_only'
ORDER BY s.bucket;

SELECT '-- an insert without histograms stores an empty histograms array, with or without the histograms.* columns in the column list';
INSERT INTO ts (metric_name, tags, samples) VALUES ('floats_only', map('job', 'api'), [(toDateTime64(100, 3), 1.)]);
INSERT INTO ts VALUES ('all_columns', map('job', 'api'), [(toDateTime64(100, 3), 2.)], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], '', '', '', '');

SELECT t.metric_name, s.samples, s.histograms
FROM timeSeriesSamples(ts) AS s JOIN timeSeriesTags(ts) AS t ON s.id = t.id
WHERE t.metric_name IN ('floats_only', 'all_columns')
ORDER BY t.metric_name;

SELECT '-- mixed integer and float counts above 2^53 choose the same winner in both input orders';
-- A: integer count 2^53, sum 3; B: integer count 2^53 + 1, sum 1; C: float count 2^53, sum 2.
-- Rounding the integer counts to `Float64` when comparing flavours makes B > A, A > C, C > B.
INSERT INTO ts (metric_name, tags, histograms.timestamp, histograms.flags, histograms.sum,
                histograms.count_int, histograms.zero_count_int, histograms.count_float, histograms.zero_count_float)
WITH [toInt64(9007199254740992), toInt64(9007199254740993), toInt64(0)] AS int_counts,
     [0., 0., 9007199254740992.] AS float_counts
SELECT concat('mixed_counts_', ordering), map(), arrayMap(i -> toDateTime64(100, 3), permutation),
       arrayMap(i -> [0, 0, 1][i], permutation), arrayMap(i -> [3., 1., 2.][i], permutation),
       arrayMap(i -> int_counts[i], permutation), arrayMap(i -> int_counts[i], permutation),
       arrayMap(i -> float_counts[i], permutation), arrayMap(i -> float_counts[i], permutation)
FROM values('ordering String, permutation Array(UInt8)', ('ABC', [1, 2, 3]), ('ACB', [1, 3, 2]));

SELECT t.metric_name, h.flags, h.count_int, h.count_float, h.sum
FROM timeSeriesSamples(ts) AS s ARRAY JOIN s.histograms AS h
JOIN timeSeriesTags(ts) AS t ON s.id = t.id
WHERE t.metric_name IN ('mixed_counts_ABC', 'mixed_counts_ACB')
ORDER BY t.metric_name;

SELECT '-- the recent samples table gets the same histograms';
-- TTL 10 days, the samples are inserted close to now(), so the background TTL cannot drop them during the test.
CREATE TABLE ts_recent ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000;
INSERT INTO ts_recent (metric_name, tags, samples, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_int)
    VALUES ('m', map('job', 'api'), [(now64(3) - INTERVAL 3 MINUTE, 1.)], [now64(3) - INTERVAL 2 MINUTE, now64(3) - INTERVAL 1 MINUTE], [1, 2], [0, 0], [[(0, 1)], [(0, 2)]]);

SELECT (SELECT (sum(length(samples)), sum(length(histograms))) FROM timeSeriesSamples(ts_recent)) AS samples_table,
       (SELECT (sum(length(samples)), sum(length(histograms))) FROM merge(currentDatabase(), '^\\.inner_id\\.recentsamples\\.')) AS recent_samples_table;

DROP TABLE ts_recent;
DROP TABLE ts;
