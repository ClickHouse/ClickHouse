SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_final_projection_buckets;

CREATE TABLE ts_final_projection_buckets ENGINE = TimeSeries
    SETTINGS version = 11, samples_bucket_step_seconds = 60, recent_samples_ttl_seconds = 0,
        tags_deduplication_cache_size_bytes = 0
    SAMPLES INNER ENGINE = AggregatingMergeTree SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1
    TAGS INNER ENGINE = ReplacingMergeTree SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1;

-- The same series spans two buckets, and the duplicate timestamp is written by a separate insert.
INSERT INTO ts_final_projection_buckets (metric_name, tags, samples) VALUES
    ('m', map('k', 'v'), [(toDateTime64(1, 3), 1.), (toDateTime64(61, 3), 2.)]);
INSERT INTO ts_final_projection_buckets (metric_name, tags, samples) VALUES
    ('m', map('k', 'v'), [(toDateTime64(1, 3), 3.)]);

-- The physical targets remain unmerged: three sample rows across two buckets and two tags rows for one `id`.
SELECT count(), uniqExact(id), uniqExact(bucket), sum(length(samples))
FROM timeSeriesSamples(ts_final_projection_buckets);
SELECT count(), uniqExact(id) FROM timeSeriesTags(ts_final_projection_buckets);

-- `FINAL` must produce one logical row for samples-only, count-only, tags-only, and joined projections.
SELECT count(), sum(length(samples))
FROM (SELECT samples FROM ts_final_projection_buckets FINAL);
SELECT count() FROM ts_final_projection_buckets FINAL;
SELECT count(), sum(length(samples)) FROM ts_final_projection_buckets FINAL;
SELECT tags['k'] FROM ts_final_projection_buckets FINAL;
SELECT tags['k'], length(samples) FROM ts_final_projection_buckets FINAL;

DROP TABLE ts_final_projection_buckets;
