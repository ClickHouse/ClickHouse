-- The `histograms_max_buckets` setting of a TimeSeries table limits the number of buckets of one histogram sample, the positive
-- and the negative side together, like Prometheus's `native_histogram_bucket_limit`; 0 means no limit. An INSERT with a bigger
-- histogram is rejected with INCORRECT_DATA (see validateTimeSeriesHistograms) and writes nothing.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_limit;
DROP TABLE IF EXISTS ts_unlimited;

-- The recent samples table is disabled because its TTL would drop the old samples.
CREATE TABLE ts_limit ENGINE = TimeSeries SETTINGS histograms_max_buckets = 3, recent_samples_ttl_seconds = 0;

SELECT '-- 3 buckets over both sides pass, 4 are rejected, for either flavour';
INSERT INTO ts_limit (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'), CAST([(toDateTime64(100, 3), 3, [(0, 1), (1, 1)], [(0, 1)])],
        'Array(Tuple(timestamp DateTime64(3), count_int Int64, positive_int Array(Tuple(Int32, Int64)), negative_int Array(Tuple(Int32, Int64))))');
INSERT INTO ts_limit (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'), CAST([(toDateTime64(101, 3), 4, [(0, 1), (1, 1)], [(0, 1), (1, 1)])],
        'Array(Tuple(timestamp DateTime64(3), count_int Int64, positive_int Array(Tuple(Int32, Int64)), negative_int Array(Tuple(Int32, Int64))))'); -- { serverError INCORRECT_DATA }
INSERT INTO ts_limit (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'), CAST([(toDateTime64(102, 3), true, 4, [(0, 1), (1, 1), (2, 1), (3, 1)])],
        'Array(Tuple(timestamp DateTime64(3), is_float Bool, count_float Float64, positive_float Array(Tuple(Int32, Float64))))'); -- { serverError INCORRECT_DATA }
INSERT INTO ts_limit (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'), CAST([(toDateTime64(103, 3), true, 3, [(0, 1), (1, 1), (2, 1)])],
        'Array(Tuple(timestamp DateTime64(3), is_float Bool, count_float Float64, positive_float Array(Tuple(Int32, Float64))))');

SELECT '-- a rejected insert writes nothing, the accepted histograms are stored';
SELECT sum(length(histograms)) FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts_limit);

SELECT '-- the limit applies to each histogram sample, not to an insert';
INSERT INTO ts_limit (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'), CAST([(toDateTime64(104, 3), 2, [(0, 1), (1, 1)]), (toDateTime64(105, 3), 2, [(0, 1), (1, 1)])],
        'Array(Tuple(timestamp DateTime64(3), count_int Int64, positive_int Array(Tuple(Int32, Int64))))');
SELECT sum(length(histograms)) FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts_limit);

SELECT '-- 0 means no limit';
CREATE TABLE ts_unlimited ENGINE = TimeSeries SETTINGS histograms_max_buckets = 0, recent_samples_ttl_seconds = 0;
INSERT INTO ts_unlimited (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'), CAST([(toDateTime64(100, 3), 10, [(0, 1), (1, 1), (2, 1), (3, 1), (4, 1), (5, 1), (6, 1), (7, 1), (8, 1), (9, 1)])],
        'Array(Tuple(timestamp DateTime64(3), count_int Int64, positive_int Array(Tuple(Int32, Int64))))');
SELECT length(h.positive_int) FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts_unlimited) ARRAY JOIN histograms AS h;

DROP TABLE ts_unlimited;
DROP TABLE ts_limit;
