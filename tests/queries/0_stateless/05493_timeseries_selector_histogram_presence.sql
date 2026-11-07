-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4.

-- `timeSeriesSelector` returns a row when the series has samples of either kind (float or histogram) in the time range, whatever
-- columns are requested: a kind which isn't requested is read through its `timestamp` subcolumn for the row filter only.
-- PromQL requests the float samples only and ignores the histogram samples.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

-- Timestamp arrays are sliced like arrays of sample tuples, including varying bounds.
WITH [100, 110, 120, 130]::Array(UInt32) AS timestamps
SELECT timeSeriesSliceSortedArray(timestamps, l, r)
FROM values('l UInt32, r UInt32', (0, 200), (105, 120), (125, 110), (135, 150)) ORDER BY l;
SELECT arrayMap(t -> toUnixTimestamp64Milli(t), timeSeriesSliceSortedArray(
    [toDateTime64(100.1, 3), toDateTime64(100.2, 3), toDateTime64(100.3, 3)], 100.15, 100.25));

DROP TABLE IF EXISTS ts_presence;
CREATE TABLE ts_presence ENGINE = TimeSeries
SETTINGS samples_bucket_step_seconds = 60, recent_samples_ttl_seconds = 0;

INSERT INTO ts_presence (metric_name, samples, histograms.timestamp, histograms.count_int, histograms.zero_count_int)
VALUES ('mixed', [(toDateTime64(100, 3), 7.)], [toDateTime64(110, 3)], [5], [5]),
       ('hist', [], [toDateTime64(100, 3), toDateTime64(110, 3)], [1, 2], [1, 2]),
       ('float', [(toDateTime64(100, 3), 3.)], [], [], []);

-- PromQL reads the timestamps of the histograms for the row filter, not the payload (whose type names `count_int`).
SELECT position(plan, 'histograms.timestamp') > 0 AND position(plan, 'count_int') = 0
FROM
(
    SELECT arrayStringConcat(groupArray(explain), '\n') AS plan
    FROM (EXPLAIN actions = 1 SELECT value FROM prometheusQuery(ts_presence, 'sum_over_time(mixed[10s])', 105))
);

-- Rows with samples of either kind in the range are returned, whatever is requested; gaps inside buckets return nothing.
SELECT count() FROM timeSeriesSelector(ts_presence, 'hist', 100, 110, 'samples');
SELECT count(), sum(length(samples)) FROM timeSeriesSelector(ts_presence, 'hist', 100, 110, 'samples');
SELECT count(), sum(length(samples)), sum(length(histograms)) FROM timeSeriesSelector(ts_presence, 'hist', 100, 100, 'samples');
SELECT count(), sum(length(histograms)) FROM timeSeriesSelector(ts_presence, 'hist', 101, 109, 'samples');
SELECT count(), sum(length(histograms)) FROM timeSeriesSelector(ts_presence, 'float', 100, 110, 'samples');
SELECT count(), sum(length(samples)), sum(length(histograms)) FROM timeSeriesSelector(ts_presence, 'mixed', 105, 110, 'samples');

-- PromQL evaluates the float samples only: the histogram at 110 is ignored, a histogram-only series gives nothing.
SELECT value FROM prometheusQuery(ts_presence, 'mixed', 115);
SELECT value FROM prometheusQuery(ts_presence, 'sum_over_time(mixed[10s])', 115);
SELECT value FROM prometheusQuery(ts_presence, 'hist', 115);
SELECT value FROM prometheusQuery(ts_presence, 'count({__name__=~"mixed|hist|float"})', 115);

DROP TABLE ts_presence;
