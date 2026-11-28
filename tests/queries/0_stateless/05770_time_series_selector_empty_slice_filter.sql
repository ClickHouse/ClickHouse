-- Tags: no-fasttest, no-parallel-replicas
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: without a local plan, the plan of a parallel replicas read does not show the filters of the replicas.

-- A row of the samples table holds the samples of one bucket. A row matching a time range by `min_time` and `max_time`
-- can have no samples in it only if the range is shorter than a bucket, and only then the selector filters the empty rows.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries SETTINGS samples_bucket_step_seconds = 600, recent_samples_ttl_seconds = 0;

-- The bucket 600 holds the samples 600 and 1199.999, the bucket 1200 the sample 1250.
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('m', map('env', 'prod'), [(toDateTime64(600, 3), 1.), (toDateTime64(1199.999, 3), 2.), (toDateTime64(1250, 3), 3.)]);

-- 599.997 seconds, inside the bucket 600 between its two samples: its row matches by min_time and max_time and is filtered out.
SELECT count() FROM timeSeriesSelector(ts, 'm', toDateTime64(600.001, 3), toDateTime64(1199.998, 3), 'samples');
-- 600 seconds: both rows have samples in the range.
SELECT count(), countIf(empty(samples)) FROM timeSeriesSelector(ts, 'm', toDateTime64(800, 3), toDateTime64(1400, 3), 'samples');

-- The filter is generated for the shorter range only.
SELECT countIf(explain ILIKE '%empty(%') > 0 FROM (EXPLAIN actions = 1 SELECT * FROM timeSeriesSelector(ts, 'm', toDateTime64(600.001, 3), toDateTime64(1199.998, 3), 'samples'));
SELECT countIf(explain ILIKE '%empty(%') > 0 FROM (EXPLAIN actions = 1 SELECT * FROM timeSeriesSelector(ts, 'm', toDateTime64(800, 3), toDateTime64(1400, 3), 'samples'));

DROP TABLE ts;
