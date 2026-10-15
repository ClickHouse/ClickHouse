-- Tags: no-async-insert, no-random-settings, no-asan, no-msan, no-tsan
-- Eight input blocks, each expanded to 100000 samples, must insert under a 40 MB limit.

SET allow_experimental_time_series_table = 1;
SET max_memory_usage = 40000000;
SET max_block_size = 1;
SET min_insert_block_size_rows = 1;
SET min_insert_block_size_bytes = 0;
SET max_insert_block_size = 1;

DROP TABLE IF EXISTS ts_blocks;

CREATE TABLE ts_blocks ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000;

INSERT INTO ts_blocks (metric_name, tags, samples)
SELECT 'm', map('n', toString(number)), arrayMap(x -> (toDateTime64('2026-01-01 00:00:00', 3), toFloat64(x)), range(100000))
FROM numbers(8);

SELECT count() FROM ts_blocks;

DROP TABLE ts_blocks;
