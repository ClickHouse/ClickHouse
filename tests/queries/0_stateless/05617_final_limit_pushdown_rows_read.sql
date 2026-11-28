-- Measure the work saved by `optimize_final_limit_pushdown` and `optimize_final_sequential_partitions`,
-- not only the plan shape: the number of rows read must drop when the optimizations are enabled.
-- If the early-termination logic in the FINAL merge algorithms (or the sequential partition `Concat`)
-- is removed, the results stay the same, but these comparisons fail.

SET enable_analyzer = 1;
SET optimize_read_in_order = 1;
SET max_threads = 1;
SET max_block_size = 1000;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET log_queries = 1;

-- ============================================================
-- Part 1: merge-level early termination.
-- Two overlapping parts with many blocks each. Without the pushdown, the `SummingSortedTransform`
-- keeps merging until it has a full output block (`max_block_size` groups), reading more source blocks.
-- With the pushdown it stops after `LIMIT` groups, reading only the first block of each part.
-- ============================================================

DROP TABLE IF EXISTS t_final_limit_rows;

CREATE TABLE t_final_limit_rows (key UInt64, value UInt64)
ENGINE = SummingMergeTree
ORDER BY key
SETTINGS index_granularity = 128;

SYSTEM STOP MERGES t_final_limit_rows;

INSERT INTO t_final_limit_rows SELECT number, 1 FROM numbers(100000);
INSERT INTO t_final_limit_rows SELECT number, 2 FROM numbers(100000);

SELECT key, value FROM t_final_limit_rows FINAL ORDER BY key LIMIT 5
SETTINGS optimize_final_limit_pushdown = 1, log_comment = '05617_merge_on';

SELECT key, value FROM t_final_limit_rows FINAL ORDER BY key LIMIT 5
SETTINGS optimize_final_limit_pushdown = 0, log_comment = '05617_merge_off';

-- ============================================================
-- Part 2: partition-level early termination.
-- Ten disjoint partitions, each merged separately (`do_not_merge_across_partitions_select_final`).
-- Virtual rows are disabled, so without the sequential `Concat` the first block of every partition is read.
-- With `optimize_final_sequential_partitions` only the first partition is read.
-- ============================================================

DROP TABLE IF EXISTS t_final_seq_rows;

CREATE TABLE t_final_seq_rows (key UInt64, value UInt64)
ENGINE = SummingMergeTree
PARTITION BY intDiv(key, 10000)
ORDER BY key
SETTINGS index_granularity = 128;

SYSTEM STOP MERGES t_final_seq_rows;

INSERT INTO t_final_seq_rows SELECT number, 1 FROM numbers(100000);
INSERT INTO t_final_seq_rows SELECT number, 2 FROM numbers(100000);

SELECT key, value FROM t_final_seq_rows FINAL ORDER BY key LIMIT 5
SETTINGS optimize_final_limit_pushdown = 1, optimize_final_sequential_partitions = 1,
         do_not_merge_across_partitions_select_final = 1, read_in_order_use_virtual_row = 0,
         log_comment = '05617_seq_on';

SELECT key, value FROM t_final_seq_rows FINAL ORDER BY key LIMIT 5
SETTINGS optimize_final_limit_pushdown = 1, optimize_final_sequential_partitions = 0,
         do_not_merge_across_partitions_select_final = 1, read_in_order_use_virtual_row = 0,
         log_comment = '05617_seq_off';

SYSTEM FLUSH LOGS query_log;

SELECT
    'merge_early_termination',
    sumIf(read_rows, log_comment = '05617_merge_on') < sumIf(read_rows, log_comment = '05617_merge_off')
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday();

-- The sequential read touches at most the first partition (2 parts x 10000 rows),
-- while the parallel read starts all ten partitions.
SELECT
    'sequential_partitions',
    sumIf(read_rows, log_comment = '05617_seq_on') <= 20000,
    sumIf(read_rows, log_comment = '05617_seq_on') < sumIf(read_rows, log_comment = '05617_seq_off')
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday();

DROP TABLE t_final_limit_rows;
DROP TABLE t_final_seq_rows;
