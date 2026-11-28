-- The merge of the adaptive aggregator converts each of its 256 buckets in units of about 16,384 groups. It
-- squashes the chunks of the units of a bucket into chunks of at least `max_block_size` rows, the last one
-- taking the rows left, so the processor after the merge takes few chunks. Units that each reach
-- `max_block_size` rows are passed on as they are. A result of aggregate states keeps a chunk per unit,
-- because squashing would copy the states one by one. Such a result is a partial one, or the final one of a
-- function like `sumState`.
--
-- `blockSize` and `rowNumberInBlock` show the chunks that the aggregation outputs: a chunk is counted at its
-- first row, where `r = 0`. Every query also checks the rows themselves. The keys are unique, so every bucket
-- holds the same number of groups whatever the scheduling, and its units split them evenly. A bucket has as
-- many partitions as the number of producers allows, which bounds its units, so every query sets
-- `max_threads`. The other settings pin what the runner randomizes: the thresholds keep every table on the
-- adaptive path, and the statistics stay off, so the result does not depend on previous runs.

SET max_threads_min_free_memory_per_thread = 0;
SET enable_adaptive_aggregator = 1;
SET adaptive_aggregator_freeze_threshold = 16384;
SET adaptive_aggregator_freeze_threshold_bytes = 0;
SET group_by_two_level_threshold = 100000000;
SET group_by_two_level_threshold_bytes = 0;
SET collect_hash_table_stats_during_aggregation = 0;
SET optimize_aggregation_in_order = 0;
SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;
SET max_rows_to_group_by = 0;

-- 13,000,000 keys are 50,781 per bucket. With 5 producers a bucket has 4 partitions, and the merge takes it in
-- 4 units of about 12,695 groups. With `max_block_size = 30000` the first three units make a chunk of about
-- 38,085 rows, which reaches the block size, and the fourth unit is left and passed on alone: 2 chunks per
-- bucket, 512 in all.
SELECT 'a run of units that reaches the block size, and a unit left over';
SELECT countIf(r = 0), countIf(r = 0 AND b >= 30000), countIf(r = 0 AND b < 30000), count(), sum(c)
FROM
(
    SELECT blockSize() AS b, rowNumberInBlock() AS r, c
    FROM (SELECT number AS k, count() AS c FROM numbers_mt(13000000) GROUP BY k)
)
SETTINGS max_threads = 5, max_block_size = 30000;

-- 9,000,000 keys are 35,156 per bucket. With 2 producers a bucket has 2 partitions, and the merge takes it in
-- 2 units of about 17,578 groups. Both together stay below `max_block_size = 65409`, so the end of the bucket
-- closes their chunk: one chunk per bucket, 256 in all, each larger than the 32,768 groups that two units hold
-- at least. The squashed rows keep their columns aligned: every key comes with its count and its own string.
SELECT 'the units of a bucket below the block size';
SELECT countIf(r = 0), minIf(b, r = 0) > 32768, count(), sum(c), countIf(toUInt64(s) = k)
FROM
(
    SELECT blockSize() AS b, rowNumberInBlock() AS r, k, c, s
    FROM (SELECT number AS k, count() AS c, any(toString(number)) AS s FROM numbers_mt(9000000) GROUP BY k)
)
SETTINGS max_threads = 2, max_block_size = 65409;

-- With `max_block_size = 15000` each of the same units reaches the block size alone, so it is passed on as it
-- is: 2 chunks per bucket, 512 in all.
SELECT 'units that reach the block size alone';
SELECT countIf(r = 0), minIf(b, r = 0) >= 15000, count(), sum(c)
FROM
(
    SELECT blockSize() AS b, rowNumberInBlock() AS r, c
    FROM (SELECT number AS k, count() AS c FROM numbers_mt(9000000) GROUP BY k)
)
SETTINGS max_threads = 2, max_block_size = 15000;

-- `WITH TOTALS` makes the result of the merge partial. The same units stay below `max_block_size = 65409`
-- together, as in the second query, but they keep a chunk each: 512 chunks, each smaller than the groups of
-- two units.
SELECT 'a partial result';
SELECT countIf(r = 0), maxIf(b, r = 0) < 32768, count(), sum(c)
FROM
(
    SELECT blockSize() AS b, rowNumberInBlock() AS r, c
    FROM (SELECT number AS k, count() AS c FROM numbers_mt(9000000) GROUP BY k WITH TOTALS)
)
SETTINGS max_threads = 2, max_block_size = 65409;

-- `sumState` returns its state, so the final result holds states as well, and its units keep a chunk each.
-- Every state still gives the sum of the only row of its key.
SELECT 'the final result of a function that returns a state';
SELECT countIf(r = 0), maxIf(b, r = 0) < 32768, count(), countIf(finalizeAggregation(s) = k)
FROM
(
    SELECT blockSize() AS b, rowNumberInBlock() AS r, k, s
    FROM (SELECT number AS k, sumState(number) AS s FROM numbers_mt(9000000) GROUP BY k)
)
SETTINGS max_threads = 2, max_block_size = 65409;
