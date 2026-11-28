-- The `native_histogram_bucket_limit` setting of a TimeSeries table limits the number of buckets of one histogram sample, the positive
-- and the negative side together, like the Prometheus scrape option of the same name; 0, the default, means no limit. Unlike in
-- Prometheus, the number of buckets of a bigger histogram isn't reduced: the INSERT is rejected with INCORRECT_DATA (see
-- validateTimeSeriesHistograms) and writes nothing. The setting can be changed with `ALTER TABLE ... MODIFY SETTING`: the limit
-- applies to the inserts started after the change (the sink reads the settings of the table when an INSERT starts), the histograms
-- already stored are not checked again.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_limit;

-- The recent samples table is disabled because its TTL would drop the old samples.
CREATE TABLE ts_limit ENGINE = TimeSeries SETTINGS native_histogram_bucket_limit = 3, recent_samples_ttl_seconds = 0;

SELECT '-- 3 buckets over both sides pass, 4 are rejected';
INSERT INTO ts_limit (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'), CAST([(toDateTime64(100, 3), 3, [(0, 1), (1, 1)], [(0, 1)])],
        'Array(Tuple(timestamp DateTime64(3), count_int Int64, positive_int Array(Tuple(Int32, Int64)), negative_int Array(Tuple(Int32, Int64))))');
INSERT INTO ts_limit (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'), CAST([(toDateTime64(101, 3), 4, [(0, 1), (1, 1)], [(0, 1), (1, 1)])],
        'Array(Tuple(timestamp DateTime64(3), count_int Int64, positive_int Array(Tuple(Int32, Int64)), negative_int Array(Tuple(Int32, Int64))))'); -- { serverError INCORRECT_DATA }

SELECT '-- a rejected insert writes nothing, the accepted histogram is stored';
SELECT sum(length(histograms)) FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts_limit);

SELECT '-- the limit applies to each histogram sample, not to an insert';
INSERT INTO ts_limit (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'), CAST([(toDateTime64(104, 3), 2, [(0, 1), (1, 1)]), (toDateTime64(105, 3), 2, [(0, 1), (1, 1)])],
        'Array(Tuple(timestamp DateTime64(3), count_int Int64, positive_int Array(Tuple(Int32, Int64))))');
SELECT sum(length(histograms)) FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts_limit);

SELECT '-- MODIFY SETTING changes the limit for the inserts started after it; the histogram with 3 buckets stored before stays';
ALTER TABLE ts_limit MODIFY SETTING native_histogram_bucket_limit = 2;
SELECT extract(create_table_query, 'native_histogram_bucket_limit = (\d+)') FROM system.tables WHERE database = currentDatabase() AND name = 'ts_limit';
INSERT INTO ts_limit (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'), CAST([(toDateTime64(106, 3), 3, [(0, 1), (1, 1), (2, 1)])],
        'Array(Tuple(timestamp DateTime64(3), count_int Int64, positive_int Array(Tuple(Int32, Int64))))'); -- { serverError INCORRECT_DATA }
INSERT INTO ts_limit (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'), CAST([(toDateTime64(107, 3), 2, [(0, 1), (1, 1)])],
        'Array(Tuple(timestamp DateTime64(3), count_int Int64, positive_int Array(Tuple(Int32, Int64))))');
SELECT arrayMap(h -> (toUnixTimestamp64Milli(h.timestamp), length(h.positive_int) + length(h.negative_int)), timeSeriesGroupArray(histograms))
FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts_limit);

SELECT '-- RESET SETTING removes the limit';
ALTER TABLE ts_limit RESET SETTING native_histogram_bucket_limit;
SELECT create_table_query LIKE '%native_histogram_bucket_limit%' FROM system.tables WHERE database = currentDatabase() AND name = 'ts_limit';
INSERT INTO ts_limit (metric_name, tags, histograms)
    SELECT 'm', map('job', 'api'), CAST([(toDateTime64(108, 3), 4, [(0, 1), (1, 1), (2, 1), (3, 1)])],
        'Array(Tuple(timestamp DateTime64(3), count_int Int64, positive_int Array(Tuple(Int32, Int64))))');
SELECT sum(length(histograms)) FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts_limit);

DROP TABLE ts_limit;
