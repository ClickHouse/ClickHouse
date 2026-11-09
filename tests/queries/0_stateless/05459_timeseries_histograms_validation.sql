-- The validation of histogram samples inserted into a TimeSeries table (see validateTimeSeriesHistograms): the rules follow
-- Prometheus's `Histogram.Validate` plus the shape rules of the storage (the pairs of a side sorted by index, only the flavour bit
-- of `flags`). An INSERT with an invalid histogram is rejected as a whole before anything is written: no tags row, no time range,
-- no samples row. The columns of the `histograms` group which an INSERT doesn't mention get default values, so a valid histogram
-- needs only a timestamp.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_val;

-- The recent samples table is disabled because its TTL would drop the old samples. Background merges of the samples table are
-- disabled so that the check of the accepted histograms doesn't depend on whether the parts of one series were merged.
CREATE TABLE ts_val ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
SAMPLES INNER ENGINE = AggregatingMergeTree SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1;

SELECT '-- only bit 0 of flags (a float histogram) may be set';
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.flags) VALUES ('m', map('job', 'api'), [toDateTime64(100, 3)], [2]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.flags) VALUES ('m', map('job', 'api'), [toDateTime64(100, 3)], [3]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.flags) VALUES ('m', map('job', 'api'), [toDateTime64(100, 3)], [128]); -- { serverError INCORRECT_DATA }

SELECT '-- counter_reset_hint is 0..3';
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.counter_reset_hint) VALUES ('m', map('job', 'api'), [toDateTime64(100, 3)], [4]); -- { serverError INCORRECT_DATA }

SELECT '-- schema is -4..8 or -53';
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.schema) VALUES ('m', map('job', 'api'), [toDateTime64(100, 3)], [9]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.schema) VALUES ('m', map('job', 'api'), [toDateTime64(100, 3)], [-5]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.schema) VALUES ('m', map('job', 'api'), [toDateTime64(100, 3)], [-52]); -- { serverError INCORRECT_DATA }

SELECT '-- the bucket indexes of a side increase strictly';
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.positive_int)
    VALUES ('m', map('job', 'api'), [toDateTime64(100, 3)], [2], [[(0, 1), (0, 1)]]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.positive_int)
    VALUES ('m', map('job', 'api'), [toDateTime64(100, 3)], [2], [[(1, 1), (0, 1)]]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.negative_int)
    VALUES ('m', map('job', 'api'), [toDateTime64(100, 3)], [2], [[(2, 1), (1, 1)]]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.flags, histograms.count_float, histograms.positive_float)
    VALUES ('m', map('job', 'api'), [toDateTime64(100, 3)], [1], [2], [[(0, 1), (0, 1)]]); -- { serverError INCORRECT_DATA }

SELECT '-- no count is negative';
-- The buckets of the integer cases add up to count_int, so only the rules on negative counts can fire.
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.positive_int)
    VALUES ('m', map('job', 'api'), [toDateTime64(100, 3)], [-1], [[(0, -1)]]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.zero_count_int) VALUES ('m', map('job', 'api'), [toDateTime64(100, 3)], [-1], [-1]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.flags, histograms.count_float) VALUES ('m', map('job', 'api'), [toDateTime64(100, 3)], [1], [-1]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.flags, histograms.zero_count_float) VALUES ('m', map('job', 'api'), [toDateTime64(100, 3)], [1], [-1]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.flags, histograms.positive_float)
    VALUES ('m', map('job', 'api'), [toDateTime64(100, 3)], [1], [[(0, -1)]]); -- { serverError INCORRECT_DATA }

SELECT '-- the buckets and the zero bucket of an integer histogram add up to count_int, or to at most count_int when sum is NaN';
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.positive_int)
    VALUES ('m', map('job', 'api'), [toDateTime64(100, 3)], [5], [[(0, 1), (1, 2)]]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_int)
    VALUES ('m', map('job', 'api'), [toDateTime64(100, 3)], [2], [nan], [[(0, 1), (1, 2)]]); -- { serverError INCORRECT_DATA }
-- The counts of the buckets overflow Int64.
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.positive_int)
    VALUES ('m', map('job', 'api'), [toDateTime64(100, 3)], [0], [[(0, 9223372036854775807), (1, 1)]]); -- { serverError INCORRECT_DATA }

SELECT '-- custom buckets: finite increasing bounds covering the positive side, no negative side, no zero bucket; exponential buckets have no bounds';
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.schema, histograms.custom_values) VALUES ('m', map('job', 'api'), [toDateTime64(100, 3)], [-53], [[nan]]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.schema, histograms.custom_values) VALUES ('m', map('job', 'api'), [toDateTime64(100, 3)], [-53], [[1, 1]]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.schema, histograms.custom_values) VALUES ('m', map('job', 'api'), [toDateTime64(100, 3)], [-53], [[2, 1]]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.schema, histograms.custom_values) VALUES ('m', map('job', 'api'), [toDateTime64(100, 3)], [-53], [[1, inf]]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.schema, histograms.custom_values) VALUES ('m', map('job', 'api'), [toDateTime64(100, 3)], [-53], [[-inf, 1]]); -- { serverError INCORRECT_DATA }
-- One bound defines two buckets (the indexes 0 and 1), but a bucket has the index 2.
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.schema, histograms.count_int, histograms.positive_int, histograms.custom_values)
    VALUES ('m', map('job', 'api'), [toDateTime64(100, 3)], [-53], [3], [[(0, 1), (1, 1), (2, 1)]], [[0.5]]); -- { serverError INCORRECT_DATA }
-- The custom buckets start at the index 0.
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.schema, histograms.count_int, histograms.positive_int, histograms.custom_values)
    VALUES ('m', map('job', 'api'), [toDateTime64(100, 3)], [-53], [1], [[(-1, 1)]], [[1]]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.schema, histograms.count_int, histograms.negative_int)
    VALUES ('m', map('job', 'api'), [toDateTime64(100, 3)], [-53], [1], [[(0, 1)]]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.schema, histograms.count_int, histograms.zero_count_int)
    VALUES ('m', map('job', 'api'), [toDateTime64(100, 3)], [-53], [1], [1]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.schema, histograms.zero_threshold)
    VALUES ('m', map('job', 'api'), [toDateTime64(100, 3)], [-53], [0.001]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.custom_values) VALUES ('m', map('job', 'api'), [toDateTime64(100, 3)], [[1]]); -- { serverError INCORRECT_DATA }

SELECT '-- nothing of a rejected insert is written';
SELECT (SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts_val)), (SELECT count() FROM timeSeriesTimeRanges({CLICKHOUSE_DATABASE:Identifier}.ts_val)), (SELECT count() FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts_val));

SELECT '-- the accepted histograms are stored as sent (one per timestamp)';
-- An empty integer histogram: the timestamp alone.
INSERT INTO ts_val (metric_name, tags, histograms.timestamp) VALUES ('m', map('job', 'api'), [toDateTime64(100, 3)]);
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.counter_reset_hint) VALUES ('m', map('job', 'api'), [toDateTime64(101, 3)], [3]);
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.schema) VALUES ('m', map('job', 'api'), [toDateTime64(102, 3)], [-4]);
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.schema) VALUES ('m', map('job', 'api'), [toDateTime64(103, 3)], [8]);
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.schema) VALUES ('m', map('job', 'api'), [toDateTime64(104, 3)], [-53]);
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.zero_count_int, histograms.positive_int)
    VALUES ('m', map('job', 'api'), [toDateTime64(105, 3)], [3], [1], [[(0, 2)]]);
-- A stale marker may have a count bigger than its buckets.
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_int)
    VALUES ('m', map('job', 'api'), [toDateTime64(106, 3)], [5], [nan], [[(0, 1), (1, 2)]]);
-- Buckets with a count of 0 are kept as sent.
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.positive_int)
    VALUES ('m', map('job', 'api'), [toDateTime64(107, 3)], [1], [[(0, 0), (1, 1), (2, 0)]]);
-- Exponential buckets may have negative indexes, and a negative side.
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.positive_int, histograms.negative_int)
    VALUES ('m', map('job', 'api'), [toDateTime64(108, 3)], [2], [[(-5, 1)]], [[(3, 1)]]);
-- The total count of a float histogram is not checked against its buckets.
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.flags, histograms.count_float, histograms.positive_float)
    VALUES ('m', map('job', 'api'), [toDateTime64(109, 3)], [1], [100], [[(0, 1)]]);
-- Three bounds define four custom buckets, the last one up to +Inf.
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.schema, histograms.count_int, histograms.positive_int, histograms.custom_values)
    VALUES ('m', map('job', 'api'), [toDateTime64(110, 3)], [-53], [15], [[(0, 1), (1, 2), (2, 4), (3, 8)]], [[0.1, 0.5, 1]]);
-- The counts go up to the maximum Int64.
INSERT INTO ts_val (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.positive_int)
    VALUES ('m', map('job', 'api'), [toDateTime64(111, 3)], [9223372036854775807], [[(0, 9223372036854775807)]]);

SELECT toUnixTimestamp64Milli(h.timestamp) AS timestamp_ms, h.flags, h.counter_reset_hint, h.schema, h.zero_threshold, h.sum,
       h.count_int, h.zero_count_int, h.positive_int, h.negative_int, h.count_float, h.positive_float, h.custom_values
FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts_val) ARRAY JOIN histograms AS h
ORDER BY h.timestamp;

DROP TABLE ts_val;
