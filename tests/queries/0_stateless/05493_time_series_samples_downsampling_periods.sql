-- The setting `samples_downsampling_periods` turns into TTL GROUP BY rules of the inner samples table:
-- a row older than the offset of a level keeps the last sample of each interval of that level.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
DROP TABLE IF EXISTS ts_copy;
DROP TABLE IF EXISTS base;

SELECT '-- the generated TTL of the inner samples table';

CREATE TABLE ts ENGINE = TimeSeries
SETTINGS samples_bucket_step_seconds = 60, recent_samples_ttl_seconds = 0, samples_downsampling_periods = '120:20,3600:60';
SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.samples.%';

SELECT '-- the stored definition is read back, a copy of the table generates the same TTL';
DETACH TABLE ts;
ATTACH TABLE ts;
CREATE TABLE ts_copy AS ts;
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.samples.%' AND engine_full LIKE '% TTL %GROUP BY id, bucket%';
DROP TABLE ts_copy;

SELECT '-- the rows past both offsets keep the last sample of each 60 second interval';

INSERT INTO ts (metric_name, tags, samples) VALUES
    ('m', map(), [(toDateTime64(0, 3), 1.), (toDateTime64(10, 3), 2.), (toDateTime64(19, 3), 3.), (toDateTime64(20, 3), 4.), (toDateTime64(45, 3), 5.), (toDateTime64(59, 3), 6.)]),
    ('m', map(), [(toDateTime64(60, 3), 7.), (toDateTime64(90, 3), 8.)]),
    ('m', map(), [(toDateTime64(130, 3), 9.), (toDateTime64(131, 3), 10.)]);

OPTIMIZE TABLE ts FINAL;
SELECT bucket, samples FROM timeSeriesSamples(ts) ORDER BY bucket;

SELECT '-- a late sample in a reduced interval is reduced by the next merge';
INSERT INTO ts (metric_name, tags, samples) VALUES ('m', map(), [(toDateTime64(100, 3), 0.)]);
OPTIMIZE TABLE ts FINAL;
SELECT bucket, samples FROM timeSeriesSamples(ts) ORDER BY bucket;

SELECT '-- a merge of a reduced row keeps it';
OPTIMIZE TABLE ts FINAL;
SELECT bucket, samples FROM timeSeriesSamples(ts) ORDER BY bucket;

SELECT '-- the rows past the first offset only keep the last sample of each 20 second interval';

CREATE TABLE base ENGINE = Memory AS SELECT toStartOfMinute(now()) - toIntervalSecond(300) AS b;
INSERT INTO ts (metric_name, tags, samples)
SELECT 'm', map(), arrayMap(i -> (toDateTime64(b, 3) + toIntervalSecond(i), toFloat64(i)), [0, 10, 19, 20, 45, 59]) FROM base;

OPTIMIZE TABLE ts FINAL;
SELECT arrayMap(x -> (toUnixTimestamp(x.timestamp) - toUnixTimestamp(b), x.value), samples)
FROM timeSeriesSamples(ts) AS t, base WHERE bucket = b;

DROP TABLE ts;
DROP TABLE base;

SELECT '-- the intervals count from the Unix epoch for any type and timezone of the timestamps: the same offsets are kept';

CREATE TABLE ts (samples Array(Tuple(UInt32, Float64))) ENGINE = TimeSeries
SETTINGS samples_bucket_step_seconds = 70, recent_samples_ttl_seconds = 0, samples_downsampling_periods = '1s:7s';
INSERT INTO ts (metric_name, tags, samples) SELECT 'm', map(), arrayMap(i -> (toUInt32(i), toFloat64(i)), range(70));
OPTIMIZE TABLE ts FINAL;
SELECT arrayMap(x -> toUnixTimestamp(x.timestamp), samples) FROM timeSeriesSamples(ts);
DROP TABLE ts;

CREATE TABLE ts (samples Array(Tuple(DateTime64(3, 'UTC'), Float64))) ENGINE = TimeSeries
SETTINGS samples_bucket_step_seconds = 70, recent_samples_ttl_seconds = 0, samples_downsampling_periods = '1s:7s';
INSERT INTO ts (metric_name, tags, samples) SELECT 'm', map(), arrayMap(i -> (toDateTime64(i, 3, 'UTC'), toFloat64(i)), range(70));
OPTIMIZE TABLE ts FINAL;
SELECT arrayMap(x -> toUnixTimestamp(x.timestamp), samples) FROM timeSeriesSamples(ts);
DROP TABLE ts;

CREATE TABLE ts (samples Array(Tuple(DateTime64(3, 'Asia/Kolkata'), Float64))) ENGINE = TimeSeries
SETTINGS samples_bucket_step_seconds = 70, recent_samples_ttl_seconds = 0, samples_downsampling_periods = '1s:7s';
INSERT INTO ts (metric_name, tags, samples) SELECT 'm', map(), arrayMap(i -> (toDateTime64(i, 3, 'Asia/Kolkata'), toFloat64(i)), range(70));
OPTIMIZE TABLE ts FINAL;
SELECT arrayMap(x -> toUnixTimestamp(x.timestamp), samples) FROM timeSeriesSamples(ts);
DROP TABLE ts;

SELECT '-- the recent samples table keeps every sample';
CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000, samples_downsampling_periods = '30d:5m';
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.recentsamples.%' AND engine_full LIKE '%GROUP BY%';
INSERT INTO ts (metric_name, tags, samples)
SELECT 'm', map(), arrayMap(i -> (toDateTime64(now(), 3) - toIntervalSecond(i * 60), toFloat64(i)), range(5));
SELECT sum(length(samples)) FROM merge(currentDatabase(), '^\\.inner_id\\.recentsamples\\.');
ALTER TABLE ts MODIFY SETTING samples_downsampling_periods = '60d:5m'; -- { serverError NOT_IMPLEMENTED }
DROP TABLE ts;

SELECT '-- invalid values';
CREATE TABLE ts ENGINE = TimeSeries SETTINGS version = 10, samples_downsampling_periods = '30d:5m'; -- { serverError INVALID_SETTING_VALUE }
CREATE TABLE ts ENGINE = TimeSeries SETTINGS samples_downsampling_periods = '30d'; -- { serverError INVALID_SETTING_VALUE }
CREATE TABLE ts ENGINE = TimeSeries SETTINGS samples_downsampling_periods = '{__name__="m"}:30d:5m'; -- { serverError INVALID_SETTING_VALUE }
CREATE TABLE ts ENGINE = TimeSeries SETTINGS samples_downsampling_periods = '30d:0s'; -- { serverError INVALID_SETTING_VALUE }
CREATE TABLE ts ENGINE = TimeSeries SETTINGS samples_downsampling_periods = '30d:7m'; -- { serverError INVALID_SETTING_VALUE }
CREATE TABLE ts ENGINE = TimeSeries SETTINGS samples_downsampling_periods = '30d:5m,20d:1h'; -- { serverError INVALID_SETTING_VALUE }
CREATE TABLE ts ENGINE = TimeSeries SETTINGS samples_downsampling_periods = '30d:5m,60d:12m'; -- { serverError INVALID_SETTING_VALUE }
CREATE TABLE ts ENGINE = TimeSeries SETTINGS samples_downsampling_periods = '3d:5m'; -- { serverError INVALID_SETTING_VALUE }
CREATE TABLE ts ENGINE = TimeSeries SETTINGS samples_downsampling_periods = '30d:5m'
SAMPLES INNER ENGINE = AggregatingMergeTree TTL toDateTime(bucket) + toIntervalDay(365); -- { serverError INCORRECT_QUERY }
