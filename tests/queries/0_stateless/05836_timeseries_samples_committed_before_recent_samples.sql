SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_bad_samples, ts_recent_token;

SELECT '--- a failed samples insert leaves no recent samples ---';
-- Each of the two blocks has one time series with 1000 samples in one hour: one row of the samples table, and 1000 rows of the
-- recent samples table with buckets of 1 second. The squashing threshold keeps back the samples of both blocks but lets the
-- recent samples through, and `input_format_max_block_wait_ms` commits them where they land.
CREATE TABLE ts_bad_samples ENGINE = TimeSeries SETTINGS samples_bucket_step_seconds = 3600, recent_samples_bucket_step_seconds = 1
    SAMPLES INNER COLUMNS (bad UInt8 MATERIALIZED throwIf(notEmpty(samples)));
INSERT INTO ts_bad_samples (metric_name, tags, samples)
SELECT 'm', map('job', 'api'),
    arrayMap(i -> (toDateTime64(toStartOfHour(now(), 'UTC') - INTERVAL 1 HOUR, 3) + toIntervalSecond(number * 1000 + i), toFloat64(i)), range(1000))
FROM numbers(2)
SETTINGS max_block_size = 1, max_insert_threads = 1, max_threads = 1, optimize_trivial_insert_select = 0,
    min_insert_block_size_rows = 0, min_insert_block_size_bytes = 48000, input_format_max_block_wait_ms = 1; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT count() FROM timeSeriesSamples(currentDatabase(), 'ts_bad_samples');
SELECT sum(rows) FROM system.parts
WHERE database = currentDatabase() AND active
    AND table = concat('.inner_id.recentsamples.', (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 'ts_bad_samples'));

SELECT '--- with a deduplication token, the samples of later blocks are kept, and a retry is deduplicated ---';
-- Without merges, each block makes a part of the samples table, and the samples written twice would be counted twice.
CREATE TABLE ts_recent_token ENGINE = TimeSeries
    SAMPLES INNER ENGINE = AggregatingMergeTree ORDER BY (id, bucket)
        SETTINGS non_replicated_deduplication_window = 100, max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO ts_recent_token (metric_name, tags, samples)
SELECT 'm', map('job', 'api'), arrayMap(i -> (toDateTime64(number * 1000 + i, 3), toFloat64(i)), range(1000)) FROM numbers(8)
SETTINGS max_block_size = 1, max_insert_threads = 1, max_threads = 1, optimize_trivial_insert_select = 0,
    min_insert_block_size_rows = 0, min_insert_block_size_bytes = 4096, insert_deduplication_token = 'ts_recent_token_insert';
INSERT INTO ts_recent_token (metric_name, tags, samples)
SELECT 'm', map('job', 'api'), arrayMap(i -> (toDateTime64(number * 1000 + i, 3), toFloat64(i)), range(1000)) FROM numbers(8)
SETTINGS max_block_size = 1, max_insert_threads = 1, max_threads = 1, optimize_trivial_insert_select = 0,
    min_insert_block_size_rows = 0, min_insert_block_size_bytes = 4096, insert_deduplication_token = 'ts_recent_token_insert';
SELECT sum(length(samples)) FROM timeSeriesSamples(currentDatabase(), 'ts_recent_token');
SELECT count() FROM system.parts
WHERE database = currentDatabase() AND active
    AND table = concat('.inner_id.samples.', (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 'ts_recent_token'));

DROP TABLE ts_bad_samples, ts_recent_token;
