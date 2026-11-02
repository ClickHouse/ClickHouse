-- The multi-pass top-K aggregation under the limits of the query. The profile events show which passes ran.
-- The passes read the input up to four times, so a limit of the rows to read must allow four times its rows,
-- and a limit of the bytes to read 40 times the decoded bytes of its columns. A group holds at least one row,
-- so a limit of the groups must allow the rows of the read. A limit that breaks the read keeps the strategy
-- off.
-- With states that grow, the candidate pass keeps the external aggregation of the query: it may flush its
-- table after any block, and it re-seeds the candidates after every flush, but it never flushes while it
-- seeds them.

SET query_plan_enable_optimizations = 1, query_plan_aggregation_bucket_top_k = 1, enable_adaptive_aggregator = 1, adaptive_aggregator_top_k = 1;
SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 64, optimize_aggregation_in_order = 0;
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, serialize_query_plan = 0;
SET group_by_two_level_threshold = 100000, group_by_two_level_threshold_bytes = 50000000;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
-- The read cannot estimate its bytes with the injection that splits its ranges, so a limit of the bytes to
-- read would keep the optimization off.
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET collect_hash_table_stats_during_aggregation = 0;

-- The table holds 67584 rows in a single part of 1056 granules of 64 rows. The optimization needs at least
-- 1024 granules, and the pilot samples every 32nd one. A query that runs no pass reads blocks of 65536 rows,
-- which leave its result unchanged: the executor takes locks for every block, and the thread fuzzer of the
-- flaky check slows down every lock.
DROP TABLE IF EXISTS t_limits;

-- The keys `heavy0` to `heavy4` hold 1 of every 20 rows each, and every other key one row.
CREATE TABLE t_limits (k String, x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_limits SELECT if(number % 20 < 5, concat('heavy', toString(number % 5)), toString(number)), number FROM numbers(67584)
SETTINGS max_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;

-- The column `k` holds less than 1 MB of decoded bytes, and the query counts less than 1 MB of read bytes.
SELECT k, count() AS c FROM t_limits GROUP BY k ORDER BY c DESC LIMIT 5 FORMAT Null SETTINGS log_comment = 'limits_01_within', max_rows_to_read = 270336, max_bytes_to_read = 1000000000, max_rows_to_group_by = 67584;
SELECT k, count() AS c FROM t_limits GROUP BY k ORDER BY c DESC LIMIT 5 FORMAT Null SETTINGS log_comment = 'limits_02_rows_to_read', max_rows_to_read = 270335, max_block_size = 65536;
SELECT k, count() AS c FROM t_limits GROUP BY k ORDER BY c DESC LIMIT 5 FORMAT Null SETTINGS log_comment = 'limits_03_bytes_to_read', max_bytes_to_read = 4000000, max_block_size = 65536;
SELECT k, count() AS c FROM t_limits GROUP BY k ORDER BY c DESC LIMIT 5 FORMAT Null SETTINGS log_comment = 'limits_04_rows_to_group_by', max_rows_to_group_by = 67583, max_block_size = 65536;
SELECT k, count() AS c FROM t_limits GROUP BY k ORDER BY c DESC LIMIT 5 FORMAT Null SETTINGS log_comment = 'limits_05_break', max_rows_to_read = 1000000, read_overflow_mode = 'break', max_block_size = 65536;

-- Every block of the candidate pass exceeds the threshold of the external aggregation. Four of the keys tie,
-- so the outer query orders them.
SELECT 'flushes of the candidate pass';
SELECT * FROM (SELECT k, count() AS c, uniqExact(x) FROM t_limits GROUP BY k ORDER BY c DESC LIMIT 5) ORDER BY k
SETTINGS log_comment = 'spills_01_candidate_pass', max_threads = 1, max_block_size = 8192, group_by_two_level_threshold_bytes = 1,
    max_bytes_before_external_group_by = 1, min_bytes_before_external_group_by = 0;
SELECT * FROM (SELECT k, count() AS c, uniqExact(x) FROM t_limits GROUP BY k ORDER BY c DESC LIMIT 5) ORDER BY k SETTINGS adaptive_aggregator_top_k = 0, max_block_size = 65536;

SYSTEM FLUSH LOGS query_log;
SELECT
    log_comment,
    ProfileEvents['AdaptiveAggregationMultiPassTopKPilotPasses'] AS pilot_passes,
    ProfileEvents['AdaptiveAggregationMultiPassTopKCandidatePasses'] AS candidate_passes,
    ProfileEvents['AdaptiveAggregationMultiPassTopKDeclines'] AS declines,
    ProfileEvents['ExternalAggregationWritePart'] > 0 AS spilled
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND match(log_comment, '^(limits|spills)_')
ORDER BY log_comment;

DROP TABLE t_limits;
