-- `optimize_final_sequential_partitions` concatenates the outputs of the partitions in order.
-- 1. A reverse read must concatenate the primary-key layers of each partition in reverse order.
-- 2. Minmax bounds of floating-point columns skip `NaN`, so they cannot prove that partitions do not overlap.

SET do_not_merge_across_partitions_select_final = 1;
SET optimize_read_in_order = 1;
SET optimize_final_limit_pushdown = 1;
SET optimize_final_sequential_partitions = 1;
SET read_in_order_use_virtual_row = 0;

DROP TABLE IF EXISTS t_seq_part_reverse_layers;

CREATE TABLE t_seq_part_reverse_layers (k UInt64, version UInt64, v UInt64)
ENGINE = ReplacingMergeTree(version)
PARTITION BY intDiv(k, 20000) ORDER BY k
SETTINGS index_granularity = 128;

SYSTEM STOP MERGES t_seq_part_reverse_layers;
INSERT INTO t_seq_part_reverse_layers SELECT number, 1, number FROM numbers(40000) SETTINGS optimize_on_insert = 0;
INSERT INTO t_seq_part_reverse_layers SELECT number, 2, number * 10 FROM numbers(40000) SETTINGS optimize_on_insert = 0;

SELECT 'reverse_layers';
SELECT k, version FROM t_seq_part_reverse_layers FINAL ORDER BY k DESC LIMIT 5
SETTINGS max_threads = 8, max_final_threads = 8, optimize_read_in_reverse_order_final = 1,
    split_intersecting_parts_ranges_into_layers_final = 1, max_block_size = 4,
    merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 1;

SELECT 'reverse_layers_forward';
SELECT k, version FROM t_seq_part_reverse_layers FINAL ORDER BY k LIMIT 5
SETTINGS max_threads = 8, max_final_threads = 8, optimize_read_in_reverse_order_final = 1,
    split_intersecting_parts_ranges_into_layers_final = 1, max_block_size = 4,
    merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read = 1;

DROP TABLE t_seq_part_reverse_layers;

DROP TABLE IF EXISTS t_seq_part_nan;

-- The first partition holds both `0` and `NaN`, the second one holds `10`.
CREATE TABLE t_seq_part_nan (k Float64, v SimpleAggregateFunction(sum, UInt64))
ENGINE = AggregatingMergeTree
PARTITION BY if(k > 5, 1, 0) ORDER BY k;

SYSTEM STOP MERGES t_seq_part_nan;
INSERT INTO t_seq_part_nan VALUES (0, 1), (10, 1), (nan, 1);
INSERT INTO t_seq_part_nan VALUES (0, 1), (10, 1), (nan, 1);

SELECT 'nan';
SELECT k, v FROM t_seq_part_nan FINAL ORDER BY k LIMIT 2 SETTINGS max_threads = 1, max_final_threads = 1;

SELECT 'nan_desc';
SELECT k, v FROM t_seq_part_nan FINAL ORDER BY k DESC LIMIT 2 SETTINGS max_threads = 1, max_final_threads = 1;

DROP TABLE t_seq_part_nan;
