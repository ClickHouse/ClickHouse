-- The multi-pass top-K aggregation of `GROUP BY ... ORDER BY uniqExact(...) DESC LIMIT n` over `MergeTree`.
-- Every query runs with the optimization and without it, which must return the same rows, and the profile
-- events show which passes ran. The cells without candidates count runs of repeated rows instead of rows,
-- which certifies more.

-- A cell counts the runs within a block, so the passes read blocks of 64 rows, one granule each, whatever
-- streams read them. A query that runs no pass reads blocks of 65536 rows, which leave its result unchanged:
-- the executor takes locks for every block, and the thread fuzzer of the flaky check slows down every lock.
-- The settings that would turn the optimization off or change the shape of the plan are pinned. The injection
-- that splits the ranges of a read is off: the read cannot estimate its bytes with it, so a limit of the
-- bytes to read would keep the optimization off.
SET query_plan_enable_optimizations = 1, query_plan_aggregation_bucket_top_k = 1, enable_adaptive_aggregator = 1, adaptive_aggregator_top_k = 1;
SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 64, optimize_aggregation_in_order = 0;
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, serialize_query_plan = 0;
SET group_by_two_level_threshold = 100000, group_by_two_level_threshold_bytes = 50000000;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET collect_hash_table_stats_during_aggregation = 0;

DROP TABLE IF EXISTS t_uniq;

-- 67584 rows in a single Wide part of 1056 granules of exactly 64 rows. The optimization needs at least 1024
-- granules, and the pilot samples every 32nd one. The insert writes one part, so no merge changes the
-- granules.
-- `r`: the keys `a0` to `a8` hold 12, 11, 10, 8, 6, 4, 3, 2 and 1 of every 199 rows, spread over the table,
-- and every other key one row, but the rows of granules 1 to 40, except 32, hold the key `bot`, which the
-- pilot never samples.
-- `v`: `id`, but the values of `bot` repeat in runs of 64 rows. It has 2496 rows, but only 39 distinct values
-- in 39 runs.
CREATE TABLE t_uniq
(
    id UInt64,
    r String,
    v UInt64
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_uniq
SELECT
    number AS id,
    if(in_block, 'bot', multiIf(m < 12, 'a0', m < 23, 'a1', m < 33, 'a2', m < 41, 'a3', m < 47, 'a4', m < 51, 'a5', m < 54, 'a6',
        m < 56, 'a7', m < 57, 'a8', concat('tail', toString(number)))) AS r,
    if(in_block, intDiv(number, 64), number) AS v
FROM (SELECT number, number % 199 AS m, number BETWEEN 64 AND 2623 AND intDiv(number, 64) != 32 AS in_block FROM numbers(67584))
SETTINGS max_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;

-- The cell of `bot` holds more rows than the fifth best key has distinct values, but fewer runs.
SELECT 'certified by runs, uniqExact';
SELECT r, uniqExact(v) AS x FROM t_uniq GROUP BY r ORDER BY x DESC LIMIT 5 SETTINGS log_comment = 'uniq_01_runs';
SELECT r, uniqExact(v) AS x FROM t_uniq GROUP BY r ORDER BY x DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0, max_block_size = 65536;

-- A row that the condition drops ends a run and does not count.
SELECT 'certified by runs, uniqExactIf';
SELECT r, uniqExactIf(v, id % 3 != 0) AS x FROM t_uniq GROUP BY r ORDER BY x DESC LIMIT 5 SETTINGS log_comment = 'uniq_02_runs_if';
SELECT r, uniqExactIf(v, id % 3 != 0) AS x FROM t_uniq GROUP BY r ORDER BY x DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0, max_block_size = 65536;

-- A condition that is not `UInt8` stays among the values. The rows of a `Nullable` column are not compared,
-- so they all count.
SELECT 'fallback, uniqExactIf with a Nullable condition';
SELECT r, uniqExactIf(v, toNullable(id % 3 != 0)) AS x FROM t_uniq GROUP BY r ORDER BY x DESC LIMIT 5 SETTINGS log_comment = 'uniq_03_fallback_nullable_condition';
SELECT r, uniqExactIf(v, toNullable(id % 3 != 0)) AS x FROM t_uniq GROUP BY r ORDER BY x DESC LIMIT 5 SETTINGS adaptive_aggregator_top_k = 0, max_block_size = 65536;

SYSTEM FLUSH LOGS query_log;
SELECT
    log_comment,
    ProfileEvents['AdaptiveAggregationMultiPassTopKPilotPasses'] AS pilot_passes,
    ProfileEvents['AdaptiveAggregationMultiPassTopKCandidatePasses'] AS candidate_passes,
    ProfileEvents['AdaptiveAggregationMultiPassTopKFallbackPasses'] AS fallback_passes,
    ProfileEvents['AdaptiveAggregationMultiPassTopKDeclines'] AS declines
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'uniq_%'
ORDER BY log_comment;

DROP TABLE t_uniq;
