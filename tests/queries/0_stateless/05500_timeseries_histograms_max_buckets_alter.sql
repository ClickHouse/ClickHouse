-- `histograms_max_buckets` can be changed with `ALTER TABLE ... MODIFY SETTING`: the limit applies to the inserts started after the
-- change (the sink reads the settings of the table when an INSERT starts), the histograms already stored are not checked again.
-- The setting exists from the version storing histogram samples, so a table of an earlier version can't get it by ALTER either.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts;

-- The recent samples table is disabled because its TTL would drop the old samples.
CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0;

SELECT '-- no limit by default: 3 buckets pass';
INSERT INTO ts (metric_name, tags, histograms)
SELECT 'm', map('job', 'api'), CAST([(toDateTime64(100, 3), 3, [(0, 1), (1, 1), (2, 1)])], 'Array(Tuple(timestamp DateTime64(3), count_int Int64, positive_int Array(Tuple(Int32, Int64))))');

SELECT '-- a limit of 2 rejects 3 buckets and accepts 2; the histogram with 3 buckets stored before stays';
ALTER TABLE ts MODIFY SETTING histograms_max_buckets = 2;
SELECT extract(create_table_query, 'histograms_max_buckets = (\d+)') FROM system.tables WHERE database = currentDatabase() AND name = 'ts';
INSERT INTO ts (metric_name, tags, histograms)
SELECT 'm', map('job', 'api'), CAST([(toDateTime64(101, 3), 3, [(0, 1), (1, 1), (2, 1)])], 'Array(Tuple(timestamp DateTime64(3), count_int Int64, positive_int Array(Tuple(Int32, Int64))))'); -- { serverError INCORRECT_DATA }
INSERT INTO ts (metric_name, tags, histograms)
SELECT 'm', map('job', 'api'), CAST([(toDateTime64(102, 3), 2, [(0, 1), (1, 1)])], 'Array(Tuple(timestamp DateTime64(3), count_int Int64, positive_int Array(Tuple(Int32, Int64))))');
SELECT arrayMap(h -> (toUnixTimestamp64Milli(h.timestamp), length(h.positive_int)), timeSeriesGroupArray(histograms)) FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts);

SELECT '-- RESET SETTING removes the limit';
ALTER TABLE ts RESET SETTING histograms_max_buckets;
SELECT create_table_query LIKE '%histograms_max_buckets%' FROM system.tables WHERE database = currentDatabase() AND name = 'ts';
INSERT INTO ts (metric_name, tags, histograms)
SELECT 'm', map('job', 'api'), CAST([(toDateTime64(103, 3), 3, [(0, 1), (1, 1), (2, 1)])], 'Array(Tuple(timestamp DateTime64(3), count_int Int64, positive_int Array(Tuple(Int32, Int64))))');
SELECT sum(length(histograms)) FROM timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts);

DROP TABLE ts;
