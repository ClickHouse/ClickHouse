-- A run of NULL keys crosses two slices of a block of the adaptive aggregator's learning phase and is followed by a non-NULL key.
-- The rows of the non-NULL key must not be added to the NULL group.

SET enable_adaptive_aggregator = 1;
SET adaptive_aggregator_freeze_threshold = 128;
SET collect_hash_table_stats_during_aggregation = 0;
SET max_block_size = 8192;

-- One stream: slices of 4096 rows, NULL keys in the first 5000 rows.
SELECT if(number < 5000, NULL, toUInt64(1)) AS k, count(), sum(number) FROM numbers(8192) GROUP BY k ORDER BY k NULLS FIRST SETTINGS max_threads = 1;
SELECT if(number < 5000, NULL, toUInt32(1)) AS k, count(), sum(number) FROM numbers(8192) GROUP BY k ORDER BY k NULLS FIRST SETTINGS max_threads = 1;
SELECT if(number < 5000, NULL, toFloat64(1)) AS k, count(), sum(number) FROM numbers(8192) GROUP BY k ORDER BY k NULLS FIRST SETTINGS max_threads = 1;

-- Several streams: the first block of every stream is sliced.
SELECT if(number % 8192 < 5000, NULL, toUInt64(1)) AS k, count(), sum(number) FROM numbers_mt(65536) GROUP BY k ORDER BY k NULLS FIRST SETTINGS max_threads = 4;

-- The default freeze threshold: slices of 16384 rows, NULL keys in the first 20000 rows.
SELECT if(number < 20000, NULL, toUInt64(1)) AS k, count(), sum(number) FROM numbers(65536) GROUP BY k ORDER BY k NULLS FIRST
SETTINGS max_threads = 1, max_block_size = 65536, adaptive_aggregator_freeze_threshold = 16384;
