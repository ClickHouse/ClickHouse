-- The validation of histogram samples inserted into a TimeSeries table (see validateTimeSeriesHistograms): the rules follow
-- Prometheus's `Histogram.Validate` plus the shape rules of the storage (the pairs of a side sorted by index without a gap wider than
-- a span offset, `is_float` and `counter_reset_hint` within their types). An INSERT with an invalid histogram is rejected as a whole before anything is written:
-- no tags row, no time range, no samples row. The histograms are inserted as named tuples with only the elements a case needs: the
-- insert gives the others their default values, so a valid histogram needs only a timestamp.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_val;

-- The recent samples table is disabled because its TTL would drop the old samples. Background merges of the samples table are
-- disabled so that the check of the accepted histograms doesn't depend on whether the parts of one series were merged.
CREATE TABLE ts_val ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
SAMPLES INNER ENGINE = AggregatingMergeTree SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1;

SELECT '-- is_float is a value of its Bool type: text input is converted to it, binary input other than 0 and 1 is rejected by the validation';
-- A binary format doesn't check the values of a Bool: an array of one histogram in RowBinary, all zeros but the timestamp 100000 and
-- `is_float` = 2. It has the type of the `histograms` column, so the insert doesn't convert it: a conversion would make it `true`.
INSERT INTO ts_val (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'), h
    FROM format(RowBinary, 'h Array(Tuple(timestamp DateTime64(3), is_float Bool, counter_reset_hint Enum8(''unknown'' = 0, ''reset'' = 1, ''not_reset'' = 2, ''gauge'' = 3), schema Int8, zero_threshold Float64, sum Float64, count_int Int64, zero_count_int Int64, count_float Float64, zero_count_float Float64, positive_int Array(Tuple(index Int32, count Int64)), negative_int Array(Tuple(index Int32, count Int64)), positive_float Array(Tuple(index Int32, count Float64)), negative_float Array(Tuple(index Int32, count Float64)), custom_values Array(Float64)))',
        unhex(concat('01', 'A086010000000000', '02', repeat('00', 2 + 6 * 8 + 5)))); -- { serverError INCORRECT_DATA }

SELECT '-- counter_reset_hint is a value of its Enum8 type: a conversion rejects other values, binary input is rejected by the validation';
INSERT INTO ts_val (metric_name, tags, histograms) SELECT 'm', map('job', 'api'), CAST([(toDateTime64(100, 3), 4)], 'Array(Tuple(timestamp DateTime64(3), counter_reset_hint Int8))'); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
-- A binary format doesn't check the values of an Enum, and the insert doesn't convert an `Enum8` of the same type: the hints 4 and -1
-- of an array of one histogram with the timestamp 100000 in RowBinary.
INSERT INTO ts_val (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'), h
    FROM format(RowBinary, 'h Array(Tuple(timestamp DateTime64(3), counter_reset_hint Enum8(''unknown'' = 0, ''reset'' = 1, ''not_reset'' = 2, ''gauge'' = 3)))', unhex('01A08601000000000004')); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'), h
    FROM format(RowBinary, 'h Array(Tuple(timestamp DateTime64(3), counter_reset_hint Enum8(''unknown'' = 0, ''reset'' = 1, ''not_reset'' = 2, ''gauge'' = 3)))', unhex('01A086010000000000FF')); -- { serverError INCORRECT_DATA }

SELECT '-- schema is -4..8 or -53';
INSERT INTO ts_val (metric_name, tags, histograms) SELECT 'm', map('job', 'api'), CAST([(toDateTime64(100, 3), 9)], 'Array(Tuple(timestamp DateTime64(3), schema Int8))'); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms) SELECT 'm', map('job', 'api'), CAST([(toDateTime64(100, 3), -5)], 'Array(Tuple(timestamp DateTime64(3), schema Int8))'); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms) SELECT 'm', map('job', 'api'), CAST([(toDateTime64(100, 3), -52)], 'Array(Tuple(timestamp DateTime64(3), schema Int8))'); -- { serverError INCORRECT_DATA }

SELECT '-- the bucket indexes of a side increase strictly';
INSERT INTO ts_val (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'),
        CAST([(toDateTime64(100, 3), 2, [(0, 1), (0, 1)])], 'Array(Tuple(timestamp DateTime64(3), count_int Int64, positive_int Array(Tuple(Int32, Int64))))'); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'),
        CAST([(toDateTime64(100, 3), 2, [(1, 1), (0, 1)])], 'Array(Tuple(timestamp DateTime64(3), count_int Int64, positive_int Array(Tuple(Int32, Int64))))'); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'),
        CAST([(toDateTime64(100, 3), 2, [(2, 1), (1, 1)])], 'Array(Tuple(timestamp DateTime64(3), count_int Int64, negative_int Array(Tuple(Int32, Int64))))'); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'),
        CAST([(toDateTime64(100, 3), true, 2, [(0, 1), (0, 1)])], 'Array(Tuple(timestamp DateTime64(3), is_float Bool, count_float Float64, positive_float Array(Tuple(Int32, Float64))))'); -- { serverError INCORRECT_DATA }

SELECT '-- the gap between two bucket indexes of a side fits the offset of a span';
-- Prometheus addresses the buckets with spans whose offset from the previous span is an Int32, so remote read couldn't return a
-- wider gap. From the index -2147483648 to 1 the offset would be 2147483648.
INSERT INTO ts_val (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'),
        CAST([(toDateTime64(100, 3), 2, [(-2147483648, 1), (1, 1)])], 'Array(Tuple(timestamp DateTime64(3), count_int Int64, negative_int Array(Tuple(Int32, Int64))))'); -- { serverError INCORRECT_DATA }

SELECT '-- no count is negative';
-- The buckets of the integer cases add up to count_int, so only the rules on negative counts can fire.
INSERT INTO ts_val (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'),
        CAST([(toDateTime64(100, 3), -1, [(0, -1)])], 'Array(Tuple(timestamp DateTime64(3), count_int Int64, positive_int Array(Tuple(Int32, Int64))))'); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms) SELECT 'm', map('job', 'api'), CAST([(toDateTime64(100, 3), -1, -1)], 'Array(Tuple(timestamp DateTime64(3), count_int Int64, zero_count_int Int64))'); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms) SELECT 'm', map('job', 'api'), CAST([(toDateTime64(100, 3), true, -1)], 'Array(Tuple(timestamp DateTime64(3), is_float Bool, count_float Float64))'); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms) SELECT 'm', map('job', 'api'), CAST([(toDateTime64(100, 3), true, -1)], 'Array(Tuple(timestamp DateTime64(3), is_float Bool, zero_count_float Float64))'); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'),
        CAST([(toDateTime64(100, 3), true, [(0, -1)])], 'Array(Tuple(timestamp DateTime64(3), is_float Bool, positive_float Array(Tuple(Int32, Float64))))'); -- { serverError INCORRECT_DATA }

SELECT '-- the buckets and the zero bucket of an integer histogram add up to count_int, or to at most count_int when sum is NaN';
INSERT INTO ts_val (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'),
        CAST([(toDateTime64(100, 3), 5, [(0, 1), (1, 2)])], 'Array(Tuple(timestamp DateTime64(3), count_int Int64, positive_int Array(Tuple(Int32, Int64))))'); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'),
        CAST([(toDateTime64(100, 3), 2, nan, [(0, 1), (1, 2)])], 'Array(Tuple(timestamp DateTime64(3), count_int Int64, sum Float64, positive_int Array(Tuple(Int32, Int64))))'); -- { serverError INCORRECT_DATA }
-- The counts of the buckets overflow Int64.
INSERT INTO ts_val (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'),
        CAST([(toDateTime64(100, 3), 0, [(0, 9223372036854775807), (1, 1)])], 'Array(Tuple(timestamp DateTime64(3), count_int Int64, positive_int Array(Tuple(Int32, Int64))))'); -- { serverError INCORRECT_DATA }

SELECT '-- custom buckets: finite increasing bounds covering the positive side, no negative side, no zero bucket; exponential buckets have no bounds';
INSERT INTO ts_val (metric_name, tags, histograms) SELECT 'm', map('job', 'api'), CAST([(toDateTime64(100, 3), -53, [nan])], 'Array(Tuple(timestamp DateTime64(3), schema Int8, custom_values Array(Float64)))'); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms) SELECT 'm', map('job', 'api'), CAST([(toDateTime64(100, 3), -53, [1, 1])], 'Array(Tuple(timestamp DateTime64(3), schema Int8, custom_values Array(Float64)))'); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms) SELECT 'm', map('job', 'api'), CAST([(toDateTime64(100, 3), -53, [2, 1])], 'Array(Tuple(timestamp DateTime64(3), schema Int8, custom_values Array(Float64)))'); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms) SELECT 'm', map('job', 'api'), CAST([(toDateTime64(100, 3), -53, [1, inf])], 'Array(Tuple(timestamp DateTime64(3), schema Int8, custom_values Array(Float64)))'); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms) SELECT 'm', map('job', 'api'), CAST([(toDateTime64(100, 3), -53, [-inf, 1])], 'Array(Tuple(timestamp DateTime64(3), schema Int8, custom_values Array(Float64)))'); -- { serverError INCORRECT_DATA }
-- One bound defines two buckets (the indexes 0 and 1), but a bucket has the index 2.
INSERT INTO ts_val (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'),
        CAST([(toDateTime64(100, 3), -53, 3, [(0, 1), (1, 1), (2, 1)], [0.5])], 'Array(Tuple(timestamp DateTime64(3), schema Int8, count_int Int64, positive_int Array(Tuple(Int32, Int64)), custom_values Array(Float64)))'); -- { serverError INCORRECT_DATA }
-- The custom buckets start at the index 0.
INSERT INTO ts_val (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'),
        CAST([(toDateTime64(100, 3), -53, 1, [(-1, 1)], [1])], 'Array(Tuple(timestamp DateTime64(3), schema Int8, count_int Int64, positive_int Array(Tuple(Int32, Int64)), custom_values Array(Float64)))'); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'),
        CAST([(toDateTime64(100, 3), -53, 1, [(0, 1)])], 'Array(Tuple(timestamp DateTime64(3), schema Int8, count_int Int64, negative_int Array(Tuple(Int32, Int64))))'); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'),
        CAST([(toDateTime64(100, 3), -53, 1, 1)], 'Array(Tuple(timestamp DateTime64(3), schema Int8, count_int Int64, zero_count_int Int64))'); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'),
        CAST([(toDateTime64(100, 3), -53, 0.001)], 'Array(Tuple(timestamp DateTime64(3), schema Int8, zero_threshold Float64))'); -- { serverError INCORRECT_DATA }
INSERT INTO ts_val (metric_name, tags, histograms) SELECT 'm', map('job', 'api'), CAST([(toDateTime64(100, 3), [1])], 'Array(Tuple(timestamp DateTime64(3), custom_values Array(Float64)))'); -- { serverError INCORRECT_DATA }

SELECT '-- nothing of a rejected insert is written';
SELECT (SELECT count() FROM timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts_val)), (SELECT count() FROM timeSeriesTimeRanges({CLICKHOUSE_DATABASE:Identifier}.ts_val)), (SELECT count() FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts_val));

SELECT '-- the accepted histograms are stored as sent (one per timestamp)';
-- An empty integer histogram: the timestamp alone.
INSERT INTO ts_val (metric_name, tags, histograms) SELECT 'm', map('job', 'api'), CAST([tuple(toDateTime64(100, 3))], 'Array(Tuple(timestamp DateTime64(3)))');
INSERT INTO ts_val (metric_name, tags, histograms) SELECT 'm', map('job', 'api'), CAST([(toDateTime64(101, 3), 'gauge')], 'Array(Tuple(timestamp DateTime64(3), counter_reset_hint String))');
INSERT INTO ts_val (metric_name, tags, histograms) SELECT 'm', map('job', 'api'), CAST([(toDateTime64(102, 3), -4)], 'Array(Tuple(timestamp DateTime64(3), schema Int8))');
INSERT INTO ts_val (metric_name, tags, histograms) SELECT 'm', map('job', 'api'), CAST([(toDateTime64(103, 3), 8)], 'Array(Tuple(timestamp DateTime64(3), schema Int8))');
INSERT INTO ts_val (metric_name, tags, histograms) SELECT 'm', map('job', 'api'), CAST([(toDateTime64(104, 3), -53)], 'Array(Tuple(timestamp DateTime64(3), schema Int8))');
INSERT INTO ts_val (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'),
        CAST([(toDateTime64(105, 3), 3, 1, [(0, 2)])], 'Array(Tuple(timestamp DateTime64(3), count_int Int64, zero_count_int Int64, positive_int Array(Tuple(Int32, Int64))))');
-- A stale marker may have a count bigger than its buckets.
INSERT INTO ts_val (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'),
        CAST([(toDateTime64(106, 3), 5, nan, [(0, 1), (1, 2)])], 'Array(Tuple(timestamp DateTime64(3), count_int Int64, sum Float64, positive_int Array(Tuple(Int32, Int64))))');
-- Buckets with a count of 0 are kept as sent.
INSERT INTO ts_val (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'),
        CAST([(toDateTime64(107, 3), 1, [(0, 0), (1, 1), (2, 0)])], 'Array(Tuple(timestamp DateTime64(3), count_int Int64, positive_int Array(Tuple(Int32, Int64))))');
-- Exponential buckets may have negative indexes, and a negative side.
INSERT INTO ts_val (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'),
        CAST([(toDateTime64(108, 3), 2, [(-5, 1)], [(3, 1)])], 'Array(Tuple(timestamp DateTime64(3), count_int Int64, positive_int Array(Tuple(Int32, Int64)), negative_int Array(Tuple(Int32, Int64))))');
-- The total count of a float histogram is not checked against its buckets.
INSERT INTO ts_val (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'),
        CAST([(toDateTime64(109, 3), true, 100, [(0, 1)])], 'Array(Tuple(timestamp DateTime64(3), is_float Bool, count_float Float64, positive_float Array(Tuple(Int32, Float64))))');
-- Three bounds define four custom buckets, the last one up to +Inf.
INSERT INTO ts_val (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'),
        CAST([(toDateTime64(110, 3), -53, 15, [(0, 1), (1, 2), (2, 4), (3, 8)], [0.1, 0.5, 1])], 'Array(Tuple(timestamp DateTime64(3), schema Int8, count_int Int64, positive_int Array(Tuple(Int32, Int64)), custom_values Array(Float64)))');
-- The counts go up to the maximum Int64.
INSERT INTO ts_val (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'),
        CAST([(toDateTime64(111, 3), 9223372036854775807, [(0, 9223372036854775807)])], 'Array(Tuple(timestamp DateTime64(3), count_int Int64, positive_int Array(Tuple(Int32, Int64))))');
-- The widest gap a span offset can express: from the index -2147483648 to 0 the offset is 2147483647, the greatest Int32.
INSERT INTO ts_val (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'),
        CAST([(toDateTime64(112, 3), 2, [(-2147483648, 1), (0, 1)])], 'Array(Tuple(timestamp DateTime64(3), count_int Int64, positive_int Array(Tuple(Int32, Int64))))');

SELECT toUnixTimestamp64Milli(h.timestamp) AS timestamp_ms, h.is_float, h.counter_reset_hint, h.schema, h.zero_threshold, h.sum,
       h.count_int, h.zero_count_int, h.positive_int, h.negative_int, h.count_float, h.positive_float, h.custom_values
FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts_val) ARRAY JOIN histograms AS h
ORDER BY h.timestamp;

DROP TABLE ts_val;
