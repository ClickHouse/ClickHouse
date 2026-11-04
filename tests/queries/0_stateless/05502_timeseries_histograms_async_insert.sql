-- An asynchronous INSERT with the `histograms.*` outer columns goes through the asynchronous insert queue: the data is parsed when
-- the queue is flushed and written by the sink the same way as by a synchronous INSERT, and an invalid histogram fails the INSERT
-- waiting for the flush.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries SETTINGS samples_bucket_step_seconds = 60, recent_samples_ttl_seconds = 0;

INSERT INTO ts (metric_name, tags, samples, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_int)
    SETTINGS async_insert = 1, wait_for_async_insert = 1
    VALUES ('m', map('job', 'api'), [(toDateTime64(100, 3), 1.)], [toDateTime64(110, 3), toDateTime64(130, 3)], [1, 2], [0.5, 1.5], [[(0, 1)], [(0, 2)]]);

SELECT t.metric_name, s.bucket, s.samples, arrayMap(h -> (h.timestamp, h.count_int, h.sum, h.positive_int), s.histograms), s.min_time, s.max_time
FROM timeSeriesSamples(ts) AS s JOIN timeSeriesTags(ts) AS t ON s.id = t.id;

-- The count of the buckets doesn't match `count_int`.
INSERT INTO ts (metric_name, tags, histograms.timestamp, histograms.count_int, histograms.positive_int)
    SETTINGS async_insert = 1, wait_for_async_insert = 1
    VALUES ('m', map('job', 'api'), [toDateTime64(140, 3)], [5], [[(0, 1)]]); -- { serverError INCORRECT_DATA }
SELECT sum(length(histograms)) FROM timeSeriesSamples(ts);

DROP TABLE ts;
