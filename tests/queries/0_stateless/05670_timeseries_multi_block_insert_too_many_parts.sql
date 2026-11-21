SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_parts;

-- The time ranges of every block are written by a new sink. A sink created for a later block must not reject
-- the insert for the parts its earlier blocks have written. The time ranges table doesn't merge its parts here.
CREATE TABLE ts_parts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
    TIME RANGES INNER ENGINE = AggregatingMergeTree ORDER BY id
    SETTINGS parts_to_throw_insert = 2, max_bytes_to_merge_at_max_space_in_pool = 1;

SELECT '--- an insert of many blocks is not rejected in the middle ---';
-- Every block below has one time series with 1000 samples, see 05239_timeseries_tags_committed_before_samples.
INSERT INTO ts_parts (metric_name, tags, samples)
SELECT 'm', map('job', 'api'), arrayMap(i -> (toDateTime64(number * 1000 + i, 3), toFloat64(i)), range(1000)) FROM numbers(8)
SETTINGS max_block_size = 1, max_insert_threads = 1, max_threads = 1, optimize_trivial_insert_select = 0,
    min_insert_block_size_rows = 0, min_insert_block_size_bytes = 4096;
SELECT max(max_time) = toDateTime64(7999, 3) FROM timeSeriesTimeRanges(currentDatabase(), 'ts_parts');
SELECT count() FROM system.parts
WHERE database = currentDatabase() AND active
    AND table = concat('.inner_id.timeranges.', (SELECT toString(uuid) FROM system.tables WHERE database = currentDatabase() AND name = 'ts_parts'));

SELECT '--- a new insert is rejected at its start ---';
INSERT INTO ts_parts (metric_name, tags, samples) VALUES ('m', {'job': 'api'}, [(toDateTime64(9000, 3), 1.)]); -- { serverError TOO_MANY_PARTS }
SELECT max(max_time) = toDateTime64(7999, 3) FROM timeSeriesTimeRanges(currentDatabase(), 'ts_parts');

DROP TABLE ts_parts;
