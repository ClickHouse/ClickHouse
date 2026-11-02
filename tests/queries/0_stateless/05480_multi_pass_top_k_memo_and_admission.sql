-- The multi-pass top-K aggregation where its runs or its passes could see different data, or exceed the
-- limits of the query. The profile events show which passes ran.
-- Candidates that an earlier run kept hold no rows once the table loses their keys. With fewer candidates
-- holding rows than the top takes, the top takes groups that no candidate covers, at any rank, also at a rank
-- of 0 that no bound of a cell excludes, so the run declines, and so do the next runs.
-- An input that a pass could read differently keeps the strategy off: a column that a part lacks and whose
-- default the reader computes in every pass, also read as a subcolumn, and a set of a table, also captured by
-- a lambda.
-- The passes read the input up to four times, so a limit of the rows to read must allow four times its rows,
-- and a limit of the bytes to read 40 times the decoded bytes of its columns. A group holds at least one row,
-- so a limit of the groups must allow the rows of the read. A limit that breaks the read keeps the strategy
-- off.
-- A run that keeps no candidates for the next runs declines when many of the best keys of the sample come in
-- runs, which hide other heavy keys between the sampled granules. A run that keeps them falls back once
-- instead.

SET query_plan_enable_optimizations = 1, query_plan_aggregation_bucket_top_k = 1, enable_adaptive_aggregator = 1, adaptive_aggregator_top_k = 1;
SET adaptive_aggregator_top_k_cache_candidates = 1;
SET max_threads = 4, max_threads_min_free_memory_per_thread = 0, max_block_size = 64, optimize_aggregation_in_order = 0;
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, serialize_query_plan = 0;
SET group_by_two_level_threshold = 100000, group_by_two_level_threshold_bytes = 50000000;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
-- The read cannot estimate its bytes with the injection that splits its ranges, so a limit of the bytes to
-- read would keep the optimization off.
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

-- Every table holds 67584 rows in a single part of 1056 granules of 64 rows. The optimization needs at least
-- 1024 granules, and the pilot samples every 32nd one.
DROP TABLE IF EXISTS t_memo;
DROP TABLE IF EXISTS t_inputs;
DROP TABLE IF EXISTS t_allowed;
DROP TABLE IF EXISTS t_runs;

CREATE TABLE t_memo (k String, x UInt64, ok UInt8) ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
-- The keys `heavy0` to `heavy4` hold 1 of every 20 rows each, and every other key one row.
INSERT INTO t_memo SELECT if(number % 20 < 5, concat('heavy', toString(number % 5)), toString(number)), number, 1 FROM numbers(67584)
SETTINGS max_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;

-- The first run keeps its candidates, the keys `heavy0` to `heavy4`, which the next runs start from.
SET collect_hash_table_stats_during_aggregation = 1;
SELECT k, uniqExactIf(x, ok) AS c FROM t_memo GROUP BY k ORDER BY c DESC LIMIT 5 FORMAT Null SETTINGS log_comment = 'memo_01_first';

-- The only key of the refill has a rank of 0, as its condition never holds.
TRUNCATE TABLE t_memo;
INSERT INTO t_memo SELECT 'only', number, 0 FROM numbers(67584)
SETTINGS max_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;
SELECT 'candidates that hold no rows';
SELECT k, uniqExactIf(x, ok) AS c FROM t_memo GROUP BY k ORDER BY c DESC LIMIT 5 SETTINGS log_comment = 'memo_02_refilled';
SELECT k, uniqExactIf(x, ok) AS c FROM t_memo GROUP BY k ORDER BY c DESC LIMIT 5 FORMAT Null SETTINGS log_comment = 'memo_03_after';

-- Without the hash table statistics, every admitted query takes its pilot.
SET collect_hash_table_stats_during_aggregation = 0;
CREATE TABLE t_inputs (k String, x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_inputs SELECT if(number % 20 < 5, concat('heavy', toString(number % 5)), toString(number)), number FROM numbers(67584)
SETTINGS max_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;
ALTER TABLE t_inputs ADD COLUMN d Tuple(flag UInt8) DEFAULT tuple(toUInt8(rand() % 2));
CREATE TABLE t_allowed (s String) ENGINE = Set;
INSERT INTO t_allowed VALUES ('heavy0'), ('heavy1');

SELECT k, count() AS c FROM t_inputs GROUP BY k ORDER BY c DESC LIMIT 5 FORMAT Null SETTINGS log_comment = 'inputs_01_plain';
SELECT k, count() AS c FROM t_inputs WHERE d.flag = 1 GROUP BY k ORDER BY c DESC LIMIT 5 FORMAT Null SETTINGS log_comment = 'inputs_02_random_default';
SELECT k, count() AS c FROM t_inputs WHERE arrayExists(s -> s IN ('heavy0', 'heavy1'), [k]) GROUP BY k ORDER BY c DESC LIMIT 5 FORMAT Null SETTINGS log_comment = 'inputs_03_lambda_literals';
SELECT k, count() AS c FROM t_inputs WHERE arrayExists(s -> s IN t_allowed, [k]) GROUP BY k ORDER BY c DESC LIMIT 5 FORMAT Null SETTINGS log_comment = 'inputs_04_lambda_table_set';

-- The column `k` holds less than 1 MB of decoded bytes, and the query counts less than 1 MB of read bytes.
SELECT k, count() AS c FROM t_inputs GROUP BY k ORDER BY c DESC LIMIT 5 FORMAT Null SETTINGS log_comment = 'limits_01_within', max_rows_to_read = 270336, max_bytes_to_read = 1000000000, max_rows_to_group_by = 67584;
SELECT k, count() AS c FROM t_inputs GROUP BY k ORDER BY c DESC LIMIT 5 FORMAT Null SETTINGS log_comment = 'limits_02_rows_to_read', max_rows_to_read = 270335;
SELECT k, count() AS c FROM t_inputs GROUP BY k ORDER BY c DESC LIMIT 5 FORMAT Null SETTINGS log_comment = 'limits_03_bytes_to_read', max_bytes_to_read = 4000000;
SELECT k, count() AS c FROM t_inputs GROUP BY k ORDER BY c DESC LIMIT 5 FORMAT Null SETTINGS log_comment = 'limits_04_rows_to_group_by', max_rows_to_group_by = 67583;
SELECT k, count() AS c FROM t_inputs GROUP BY k ORDER BY c DESC LIMIT 5 FORMAT Null SETTINGS log_comment = 'limits_05_break', max_rows_to_read = 1000000, read_overflow_mode = 'break';

CREATE TABLE t_runs (k String, x UInt64) ENGINE = MergeTree ORDER BY x
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
-- The keys `s0` to `s9` hold 24 to 33 of every 1009 rows, spread over the table. The keys `c0` to `c9` come
-- in runs of 36 to 45 rows within sampled granules, each in one of them. The keys `h0` to `h9` come in runs
-- of 300 to 660 rows between the sampled granules, so the sample misses them, although they make half of the
-- best 20 keys. Every other key holds one row.
INSERT INTO t_runs
SELECT
    multiIf(in_run, concat('c', toString(block - 1)), in_hidden, concat('h', toString(block - 1)),
        m % 30 < 10 AND intDiv(m, 30) < 24 + m % 30, concat('s', toString(m % 30)), concat('t', toString(number))),
    number
FROM
(
    SELECT number, number % 1009 AS m, intDiv(number, 2048) AS block, number % 2048 AS offset,
        block BETWEEN 1 AND 10 AND offset < 36 + (block - 1) AS in_run,
        block BETWEEN 1 AND 10 AND offset >= 256 AND offset < 256 + 300 + 40 * (block - 1) AS in_hidden
    FROM numbers(67584)
)
SETTINGS max_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1;

SELECT 'keys in runs';
SELECT groupArray((k, c)) FROM (SELECT k, count() AS c FROM t_runs GROUP BY k ORDER BY c DESC LIMIT 20) SETTINGS log_comment = 'runs_01_not_kept';
SELECT groupArray((k, c)) FROM (SELECT k, count() AS c FROM t_runs GROUP BY k ORDER BY c DESC LIMIT 20) SETTINGS log_comment = 'runs_02_kept', collect_hash_table_stats_during_aggregation = 1;
SELECT groupArray((k, c)) FROM (SELECT k, count() AS c FROM t_runs GROUP BY k ORDER BY c DESC LIMIT 20) SETTINGS adaptive_aggregator_top_k = 0;

SYSTEM FLUSH LOGS query_log;
SELECT
    log_comment,
    ProfileEvents['AdaptiveAggregationMultiPassTopKPilotPasses'] AS pilot_passes,
    ProfileEvents['AdaptiveAggregationMultiPassTopKCandidatePasses'] AS candidate_passes,
    ProfileEvents['AdaptiveAggregationMultiPassTopKFallbackPasses'] AS fallback_passes,
    ProfileEvents['AdaptiveAggregationMultiPassTopKDeclines'] AS declines
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND match(log_comment, '^(memo|inputs|limits|runs)_')
ORDER BY log_comment;

DROP TABLE t_memo;
DROP TABLE t_inputs;
DROP TABLE t_allowed;
DROP TABLE t_runs;
