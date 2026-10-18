-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas: the statistics are loaded on the initiator only, and the test counts the loads.

-- The statistics of many parts are loaded in parallel (up to `max_threads` threads) by the part pruning,
-- the ordering of PREWHERE conditions and `min`/`max`/`count` from statistics. The results must not depend
-- on the number of threads.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
SET use_query_cache = 0, use_query_condition_cache = 0;
SET materialize_statistics_on_insert = 1;
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, allow_reorder_prewhere_conditions = 1;
SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SET optimize_aggregation_in_order = 0, force_aggregation_in_order = 0, aggregate_functions_null_for_empty = 0;

DROP TABLE IF EXISTS t_stats_parallel_load;

CREATE TABLE t_stats_parallel_load (a UInt64, b UInt64, c UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';

SYSTEM STOP MERGES t_stats_parallel_load;

-- 50 parts of 100 rows each, the part `i` has the values of `a` from `100 * i` to `100 * i + 99`.
INSERT INTO t_stats_parallel_load SELECT number, number % 7, number % 100 FROM numbers(5000)
SETTINGS max_block_size = 100, min_insert_block_size_rows = 100, min_insert_block_size_bytes = 0, max_insert_threads = 1;

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_stats_parallel_load' AND active;

-- Part pruning by statistics: only the last 5 parts can match.
SELECT count(), sum(a) FROM t_stats_parallel_load WHERE a >= 4500 AND b = 3
SETTINGS use_statistics = 0, use_statistics_for_part_pruning = 1, max_threads = 8, log_comment = 'stats_parallel_load_pruning_parallel';
SELECT count(), sum(a) FROM t_stats_parallel_load WHERE a >= 4500 AND b = 3
SETTINGS use_statistics = 0, use_statistics_for_part_pruning = 1, max_threads = 1, log_comment = 'stats_parallel_load_pruning_sequential';
SELECT count(), sum(a) FROM t_stats_parallel_load WHERE a >= 4500 AND b = 3
SETTINGS use_statistics = 0, use_statistics_for_part_pruning = 0, max_threads = 8, log_comment = 'stats_parallel_load_pruning_off';

-- Ordering of PREWHERE conditions by statistics: the statistics of every part are loaded.
SELECT count(), sum(a) FROM t_stats_parallel_load WHERE b = 3 AND c < 10 AND a % 3 = 0
SETTINGS use_statistics = 1, use_statistics_for_part_pruning = 0, max_threads = 8, log_comment = 'stats_parallel_load_prewhere_parallel';
SELECT count(), sum(a) FROM t_stats_parallel_load WHERE b = 3 AND c < 10 AND a % 3 = 0
SETTINGS use_statistics = 1, use_statistics_for_part_pruning = 0, max_threads = 1, log_comment = 'stats_parallel_load_prewhere_sequential';

-- Both: the statistics of the parts left after pruning are loaded for the PREWHERE.
SELECT count(), sum(a) FROM t_stats_parallel_load WHERE a >= 4500 AND b = 3 AND c < 50
SETTINGS use_statistics = 1, use_statistics_for_part_pruning = 1, max_threads = 8, log_comment = 'stats_parallel_load_both_parallel';

-- `min`, `max` and `count` from statistics: no part is read.
SELECT min(a), max(a), min(c), max(c), count() FROM t_stats_parallel_load
SETTINGS use_statistics_for_min_max_aggregation = 1, max_threads = 8, log_comment = 'stats_parallel_load_min_max_parallel';
SELECT min(a), max(a), min(c), max(c), count() FROM t_stats_parallel_load
SETTINGS use_statistics_for_min_max_aggregation = 1, max_threads = 1, log_comment = 'stats_parallel_load_min_max_sequential';
SELECT min(a), max(a), min(c), max(c), count() FROM t_stats_parallel_load
SETTINGS use_statistics_for_min_max_aggregation = 0, max_threads = 8, log_comment = 'stats_parallel_load_min_max_off';

SYSTEM FLUSH LOGS query_log;

SELECT
    replaceOne(log_comment, 'stats_parallel_load_', '') AS query,
    ProfileEvents['SelectedParts'] AS selected_parts,
    ProfileEvents['LoadedStatistics'] AS loaded_statistics
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND startsWith(log_comment, 'stats_parallel_load_')
ORDER BY event_time_microseconds;

DROP TABLE t_stats_parallel_load;
