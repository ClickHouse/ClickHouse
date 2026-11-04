-- `INSERT ... SELECT *` copies a TimeSeries table storing native histograms into another one: the positional insert carries the
-- flattened `histograms.*` group along with the other outer columns, and the copy stores the same samples of both kinds, the counter
-- reset hints included (the hint of the first sample of a series is kept as sent, the others are detected again from the same data).

SET enable_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_src;
DROP TABLE IF EXISTS ts_dst;

CREATE TABLE ts_src ENGINE = TimeSeries SETTINGS samples_bucket_step_seconds = 60, recent_samples_ttl_seconds = 0;

-- A series with float samples and integer histograms in one bucket, a float-only series, and a gauge float histogram with custom buckets.
INSERT INTO ts_src (metric_name, tags, samples, histograms.timestamp, histograms.count_int, histograms.sum, histograms.positive_int)
    VALUES ('mixed', map('job', 'api'), [(toDateTime64(130, 3), 1.5)], [toDateTime64(125, 3), toDateTime64(145, 3)], [1, 2], [0.5, 1.5], [[(0, 1)], [(0, 2)]]);
INSERT INTO ts_src (metric_name, tags, samples) VALUES ('floats_only', map('job', 'api'), [(toDateTime64(100, 3), 1.)]);
INSERT INTO ts_src (metric_name, tags, histograms.timestamp, histograms.flags, histograms.counter_reset_hint, histograms.schema, histograms.sum,
                    histograms.count_float, histograms.positive_float, histograms.custom_values)
    VALUES ('float_custom', map('job', 'api'), [toDateTime64(100, 3)], [1], [3], [-53], [3.25], [2.5], [[(0, 1.5), (1, 1.)]], [[1.]]);

CREATE TABLE ts_dst AS ts_src;
INSERT INTO ts_dst SELECT * FROM ts_src;

SELECT '-- the copy has the rows of the source';
SELECT metric_name, samples, histograms.timestamp, histograms.flags, histograms.counter_reset_hint, histograms.schema, histograms.sum,
       histograms.count_int, histograms.positive_int, histograms.count_float, histograms.positive_float, histograms.custom_values
FROM ts_dst ORDER BY ALL;

SELECT '-- nothing differs between the source and the copy';
SELECT count() FROM (SELECT * FROM ts_src EXCEPT SELECT * FROM ts_dst);
SELECT count() FROM (SELECT * FROM ts_dst EXCEPT SELECT * FROM ts_src);

DROP TABLE ts_dst;
DROP TABLE ts_src;
