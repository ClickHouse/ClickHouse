-- Inserting native histogram samples into a TimeSeries table through the outer column `histograms` (see TimeSeriesSink):
-- the sink sorts and deduplicates them per series, splits them into the buckets of the samples tables together with the float
-- samples (a row holds the float samples and the histogram samples of one series within one bucket), and writes the time ranges.
-- The stored rows are checked through the inner tables, so the checks depend on the sink only (reading the outer column
-- is covered by 05461_timeseries_histograms_outer_read).
-- Every case uses a metric name of its own, so the rows of a case never share a key with another part and the checks don't
-- depend on background merges. The histograms are inserted as named tuples with only the elements a case needs: the insert
-- matches the elements by name and gives the others their default values.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
DROP TABLE IF EXISTS ts_recent;

-- The buckets are 60 seconds long; the recent samples table is disabled because its TTL would drop the old samples.
CREATE TABLE ts ENGINE = TimeSeries SETTINGS samples_bucket_step_seconds = 60, recent_samples_ttl_seconds = 0;

SELECT '-- an integer histogram and a float histogram, stored as sent but for the hint of the first sample of a row: unknown unless a gauge';
INSERT INTO ts (metric_name, tags, histograms)
SELECT 'int_hist', map('job', 'api'),
    CAST([(toDateTime64(100, 3), false, 'not_reset', 3, 0.001, 12.5, 10, 2, [(-2, 3), (-1, 2), (1, 3)], [(0, 0)])],
         'Array(Tuple(timestamp DateTime64(3), is_float Bool, counter_reset_hint String, schema Int8, zero_threshold Float64, sum Float64,
                      count_int Int64, zero_count_int Int64, positive_int Array(Tuple(Int32, Int64)), negative_int Array(Tuple(Int32, Int64))))');
INSERT INTO ts (metric_name, tags, histograms)
SELECT 'float_hist', map('job', 'api'),
    CAST([(toDateTime64(100, 3), true, 'gauge', 0, 3.25, 5.75, 0.5, [(0, 2.5), (1, 2.5)], [(1, 0.25)])],
         'Array(Tuple(timestamp DateTime64(3), is_float Bool, counter_reset_hint String, schema Int8, sum Float64,
                      count_float Float64, zero_count_float Float64, positive_float Array(Tuple(Int32, Float64)), negative_float Array(Tuple(Int32, Float64))))');

SELECT t.metric_name, toUnixTimestamp64Milli(h.timestamp) AS timestamp_ms, h.is_float, h.counter_reset_hint, h.schema, h.zero_threshold, h.sum,
       h.count_int, h.zero_count_int, h.positive_int, h.negative_int, h.count_float, h.zero_count_float, h.positive_float, h.negative_float, h.custom_values,
       length(s.samples)
FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts) AS s ARRAY JOIN s.histograms AS h
JOIN timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts) AS t ON s.id = t.id
WHERE t.metric_name IN ('int_hist', 'float_hist')
ORDER BY t.metric_name;

SELECT '-- the float samples and the histograms of one series within one bucket make one row, its time range covers both kinds';
INSERT INTO ts (metric_name, tags, samples, histograms)
SELECT 'mixed', map('job', 'api'), [(toDateTime64(130, 3), 1.5), (toDateTime64(150, 3), 2.5)],
    CAST([(toDateTime64(125, 3), 1, 0.5, [(0, 1)]), (toDateTime64(145, 3), 2, 1.5, [(0, 2)])],
         'Array(Tuple(timestamp DateTime64(3), count_int Int64, sum Float64, positive_int Array(Tuple(Int32, Int64))))');

SELECT t.metric_name, s.bucket, s.min_time, s.max_time, s.samples, s.histograms.timestamp, s.histograms.count_int
FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts) AS s JOIN timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts) AS t ON s.id = t.id
WHERE t.metric_name = 'mixed'
ORDER BY s.bucket;

SELECT '-- the samples of several buckets make a row per bucket, each with the float samples and the histograms of its bucket';
INSERT INTO ts (metric_name, tags, samples, histograms)
SELECT 'buckets', map('job', 'api'), [(toDateTime64(70, 3), 1.), (toDateTime64(130, 3), 2.), (toDateTime64(190, 3), 3.)],
    CAST([(toDateTime64(80, 3), 1, [(0, 1)]), (toDateTime64(140, 3), 2, [(0, 2)]), (toDateTime64(170, 3), 3, [(0, 3)]), (toDateTime64(250, 3), 4, [(0, 4)])],
         'Array(Tuple(timestamp DateTime64(3), count_int Int64, positive_int Array(Tuple(Int32, Int64))))');

SELECT t.metric_name, s.bucket, s.min_time, s.max_time, s.samples, s.histograms.timestamp, s.histograms.count_int
FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts) AS s JOIN timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts) AS t ON s.id = t.id
WHERE t.metric_name = 'buckets'
ORDER BY s.bucket;

SELECT '-- unsorted histograms are sorted; of duplicates by timestamp the greatest count wins (counts compared exactly across flavours, a NaN count loses), then the greatest sum, then the other elements; never the input order';
-- Timestamp 20: equal counts, the greater sum comes first in the input. Timestamp 50: equal counts and sums, the pairs decide
-- (the count of the bucket 0: 2 beats 1), again against the input order. See TimeSeriesDuplicateTimestampRule.
INSERT INTO ts (metric_name, tags, histograms)
SELECT 'dups', map('job', 'api'),
    CAST([(toDateTime64(30, 3), false, 5, 0, 1, [(0, 5)], []),
          (toDateTime64(10, 3), true, 0, 4.5, 2, [], [(0, 4.5)]),
          (toDateTime64(20, 3), false, 7, 0, 6, [(0, 7)], []),
          (toDateTime64(10, 3), false, 5, 0, 4, [(0, 5)], []),
          (toDateTime64(30, 3), false, 4, 0, 5, [(0, 4)], []),
          (toDateTime64(20, 3), false, 7, 0, 3, [(0, 7)], []),
          (toDateTime64(40, 3), false, 1, 0, 7, [(0, 1)], []),
          (toDateTime64(40, 3), true, 0, nan, 8, [], []),
          (toDateTime64(50, 3), false, 3, 0, 9, [(0, 2), (1, 1)], []),
          (toDateTime64(50, 3), false, 3, 0, 9, [(0, 1), (1, 2)], [])],
         'Array(Tuple(timestamp DateTime64(3), is_float Bool, count_int Int64, count_float Float64, sum Float64,
                      positive_int Array(Tuple(Int32, Int64)), positive_float Array(Tuple(Int32, Float64))))');

SELECT t.metric_name, toUnixTimestamp64Milli(h.timestamp) AS timestamp_ms, h.is_float, h.count_int, h.count_float, h.sum, h.positive_int, h.positive_float
FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts) AS s ARRAY JOIN s.histograms AS h
JOIN timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts) AS t ON s.id = t.id
WHERE t.metric_name = 'dups'
ORDER BY h.timestamp;

SELECT '-- the time range of a series covers its float samples and its histograms';
SELECT t.metric_name, min(r.min_time), max(r.max_time)
FROM timeSeriesTimeRanges({CLICKHOUSE_DATABASE:Identifier}.ts) AS r JOIN timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts) AS t ON r.id = t.id
GROUP BY t.metric_name
ORDER BY t.metric_name;

SELECT '-- errors: a histogram needs a series';
INSERT INTO ts (histograms) SELECT CAST([(toDateTime64(100, 3), 0)], 'Array(Tuple(timestamp DateTime64(3), count_int Int64))'); -- { serverError INCORRECT_DATA }

SELECT '-- a timestamp alone is a valid empty integer histogram: the other elements default to zeros and empty arrays';
INSERT INTO ts (metric_name, tags, histograms)
SELECT 'timestamps_only', map('job', 'api'), CAST([tuple(toDateTime64(100, 3)), tuple(toDateTime64(160, 3))], 'Array(Tuple(timestamp DateTime64(3)))');

SELECT t.metric_name, s.bucket, length(s.samples), s.histograms.timestamp, s.histograms.is_float, s.histograms.count_int, s.histograms.positive_int
FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts) AS s JOIN timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts) AS t ON s.id = t.id
WHERE t.metric_name = 'timestamps_only'
ORDER BY s.bucket;

SELECT '-- an insert without histograms stores an empty histograms array, with or without the histograms column in the column list';
INSERT INTO ts (metric_name, tags, samples) VALUES ('floats_only', map('job', 'api'), [(toDateTime64(100, 3), 1.)]);
INSERT INTO ts VALUES ('all_columns', map('job', 'api'), [(toDateTime64(100, 3), 2.)], [], '', '', '', '');

SELECT t.metric_name, s.samples, s.histograms
FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts) AS s JOIN timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts) AS t ON s.id = t.id
WHERE t.metric_name IN ('floats_only', 'all_columns')
ORDER BY t.metric_name;

SELECT '-- mixed integer and float counts above 2^53 choose the same winner in both input orders';
-- A: integer count 2^53, sum 3; B: integer count 2^53 + 1, sum 1; C: float count 2^53, sum 2.
-- Rounding the integer counts to `Float64` when comparing flavours makes B > A, A > C, C > B.
INSERT INTO ts (metric_name, tags, histograms)
WITH [toInt64(9007199254740992), toInt64(9007199254740993), toInt64(0)] AS int_counts,
     [0., 0., 9007199254740992.] AS float_counts
SELECT concat('mixed_counts_', ordering), map(),
       CAST(arrayMap(i -> (toDateTime64(100, 3), [0, 0, 1][i], [3., 1., 2.][i], int_counts[i], int_counts[i], float_counts[i], float_counts[i]), permutation),
            'Array(Tuple(timestamp DateTime64(3), is_float Bool, sum Float64, count_int Int64, zero_count_int Int64, count_float Float64, zero_count_float Float64))')
FROM values('ordering String, permutation Array(UInt8)', ('ABC', [1, 2, 3]), ('ACB', [1, 3, 2]));

SELECT t.metric_name, h.is_float, h.count_int, h.count_float, h.sum
FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts) AS s ARRAY JOIN s.histograms AS h
JOIN timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts) AS t ON s.id = t.id
WHERE t.metric_name IN ('mixed_counts_ABC', 'mixed_counts_ACB')
ORDER BY t.metric_name;

SELECT '-- the recent samples table gets the same histograms';
-- TTL 10 days, the samples are inserted close to now(), so the background TTL cannot drop them during the test.
CREATE TABLE ts_recent ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000;
INSERT INTO ts_recent (metric_name, tags, samples, histograms)
SELECT 'm', map('job', 'api'), [(now64(3) - INTERVAL 3 MINUTE, 1.)],
    CAST([(now64(3) - INTERVAL 2 MINUTE, 1, [(0, 1)]), (now64(3) - INTERVAL 1 MINUTE, 2, [(0, 2)])],
         'Array(Tuple(timestamp DateTime64(3), count_int Int64, positive_int Array(Tuple(Int32, Int64))))');

SELECT (SELECT (sum(length(samples)), sum(length(histograms))) FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts_recent)) AS samples_table,
       (SELECT (sum(length(samples)), sum(length(histograms))) FROM merge(currentDatabase(), '^\\.inner_id\\.recentsamples\\.')) AS recent_samples_table;

DROP TABLE ts_recent;
DROP TABLE ts;
