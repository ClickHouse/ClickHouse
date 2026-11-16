-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4.

-- For a time range shorter than a bucket, as in all the `timeSeriesSelector` queries below, the function returns a row when the
-- series has samples of a kind named by `return_columns` in the time range: 'samples' the rows with float samples, 'histograms'
-- the rows with histogram samples, 'samples_and_histograms' the rows with either. PromQL reads the float samples only, with
-- 'samples', and ignores the histogram samples.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_presence;
CREATE TABLE ts_presence ENGINE = TimeSeries
SETTINGS samples_bucket_step_seconds = 60, recent_samples_ttl_seconds = 0;

INSERT INTO ts_presence (metric_name, samples, histograms.timestamp, histograms.count_int, histograms.zero_count_int)
VALUES ('mixed', [(toDateTime64(100, 3), 7.)], [toDateTime64(110, 3)], [5], [5]),
       ('hist', [], [toDateTime64(100, 3), toDateTime64(110, 3)], [1, 2], [1, 2]),
       ('float', [(toDateTime64(100, 3), 3.)], [], [], []);

-- PromQL doesn't read the histograms.
SELECT position(plan, 'histograms') = 0
FROM
(
    SELECT arrayStringConcat(groupArray(explain), '\n') AS plan
    FROM (EXPLAIN actions = 1 SELECT value FROM prometheusQuery(ts_presence, 'sum_over_time(mixed[10s])', 105))
);

-- The rows with samples of the returned kinds in the range; gaps inside buckets return nothing.
SELECT count(), sum(length(samples)) FROM timeSeriesSelector(ts_presence, 'hist', 100, 110, 'samples');
SELECT count(), sum(length(histograms)) FROM timeSeriesSelector(ts_presence, 'hist', 100, 110, 'histograms');
SELECT count(), sum(length(samples)), sum(length(histograms)) FROM timeSeriesSelector(ts_presence, 'hist', 100, 100, 'samples_and_histograms');
SELECT count(), sum(length(histograms)) FROM timeSeriesSelector(ts_presence, 'hist', 101, 109, 'histograms');
SELECT count(), sum(length(histograms)) FROM timeSeriesSelector(ts_presence, 'float', 100, 110, 'histograms');
SELECT count(), sum(length(samples)), sum(length(histograms)) FROM timeSeriesSelector(ts_presence, 'float', 100, 110, 'samples_and_histograms');
SELECT count(), sum(length(samples)) FROM timeSeriesSelector(ts_presence, 'mixed', 105, 110, 'samples');
SELECT count(), sum(length(samples)), sum(length(histograms)) FROM timeSeriesSelector(ts_presence, 'mixed', 105, 110, 'samples_and_histograms');

-- PromQL evaluates the float samples only: the histogram at 110 is ignored, a histogram-only series gives nothing.
SELECT value FROM prometheusQuery(ts_presence, 'mixed', 115);
SELECT value FROM prometheusQuery(ts_presence, 'sum_over_time(mixed[10s])', 115);
SELECT value FROM prometheusQuery(ts_presence, 'hist', 115);
SELECT value FROM prometheusQuery(ts_presence, 'count({__name__=~"mixed|hist|float"})', 115);

DROP TABLE ts_presence;
