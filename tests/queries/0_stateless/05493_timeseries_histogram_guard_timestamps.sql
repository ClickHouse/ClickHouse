-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

-- Timestamp arrays use the same slicing implementation as arrays of sample tuples, including varying bounds.
WITH [100, 110, 120, 130]::Array(UInt32) AS timestamps
SELECT timeSeriesSliceSortedArray(timestamps, l, r)
FROM values('l UInt32, r UInt32', (0, 200), (105, 120), (125, 110), (135, 150)) ORDER BY l;
SELECT arrayMap(t -> toUnixTimestamp64Milli(t), timeSeriesSliceSortedArray(
    [toDateTime64(100.1, 3), toDateTime64(100.2, 3), toDateTime64(100.3, 3)], 100.15, 100.25));

DROP TABLE IF EXISTS ts_histogram_guard;
CREATE TABLE ts_histogram_guard ENGINE = TimeSeries
SETTINGS samples_bucket_step_seconds = 60, recent_samples_ttl_seconds = 0;

INSERT INTO ts_histogram_guard (metric_name, samples, histograms.timestamp, histograms.count_int, histograms.zero_count_int)
VALUES ('mixed', [(toDateTime64(100, 3), 7.)], [toDateTime64(110, 3)], [5], [5]),
       ('hist', [], [toDateTime64(100, 3), toDateTime64(110, 3)], [1, 2], [1, 2]),
       ('float', [(toDateTime64(100, 3), 3.)], [], [], []);

-- The guard must read timestamps from the samples table, without materializing the histogram payload.
SELECT position(plan, 'histograms.timestamp') > 0 AND position(plan, 'count_int') = 0
FROM
(
    SELECT arrayStringConcat(groupArray(explain), '\n') AS plan
    FROM (EXPLAIN actions = 1 SELECT value FROM prometheusQuery(ts_histogram_guard, 'sum_over_time(mixed[10s])', 105))
);

-- A histogram elsewhere in the same bucket must not trip the guard.
SELECT value FROM prometheusQuery(ts_histogram_guard, 'mixed', 105);
SELECT value FROM prometheusQuery(ts_histogram_guard, 'sum_over_time(mixed[10s])', 105);
SELECT value FROM prometheusQuery(ts_histogram_guard, 'mixed', 115); -- { serverError NOT_IMPLEMENTED }
SELECT value FROM prometheusQuery(ts_histogram_guard, 'sum_over_time(hist[10s])', 115); -- { serverError NOT_IMPLEMENTED }

-- Projection must preserve histogram-only and float-only rows, and discard gaps inside buckets.
SELECT count() FROM timeSeriesSelector(ts_histogram_guard, 'hist', 100, 110, 'samples');
SELECT count(), sum(length(samples)), sum(length(histograms.timestamp))
FROM timeSeriesSelector(ts_histogram_guard, 'hist', 100, 100, 'samples');
SELECT count(), sum(length(histograms.timestamp))
FROM timeSeriesSelector(ts_histogram_guard, 'hist', 101, 109, 'samples');
SELECT count(), sum(length(histograms.timestamp))
FROM timeSeriesSelector(ts_histogram_guard, 'float', 100, 110, 'samples');

-- The explicit timestamp request works with automatic subcolumn optimizations disabled, and alongside the full payload.
SELECT arrayMap(t -> toUnixTimestamp64Milli(t), histograms.timestamp)
FROM timeSeriesSelector(ts_histogram_guard, 'hist', 100, 100, 'samples')
SETTINGS optimize_functions_to_subcolumns = 0;
SELECT histograms.timestamp = arrayMap(h -> h.timestamp, histograms), arrayMap(h -> h.count_int, histograms)
FROM timeSeriesSelector(ts_histogram_guard, 'hist', 100, 110, 'samples');

DROP TABLE ts_histogram_guard;
