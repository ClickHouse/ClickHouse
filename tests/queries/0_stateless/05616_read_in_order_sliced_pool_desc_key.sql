-- Tags: no-parallel-replicas, no-object-storage
-- The sliced pool is used for local reading only; the bounds below assume the slice sizes of parts
-- on local disks.

-- A sorting key with a descending column: the pool queues the lanes by the key at their next mark in
-- the order of the key itself, so a query answered within the first part in that order leaves the parts
-- after it untouched, whichever direction it reads in.

SET optimize_read_in_order = 1;
SET read_in_order_use_virtual_row = 1;
SET read_in_order_use_sliced_pool = 1;
SET read_in_order_two_level_merge_threshold = 100;
SET use_query_condition_cache = 0;
SET use_skip_indexes_for_top_k = 0;
SET use_top_k_dynamic_filtering = 0;
SET materialize_statistics_on_insert = 0;
SET max_block_size = 1024;
SET max_insert_threads = 1;
-- Slices of at most 4 marks, so that a part is read as many slices.
SET merge_tree_min_rows_for_concurrent_read = 512;
SET merge_tree_min_bytes_for_concurrent_read = 1;
SET max_threads = 8;

DROP TABLE IF EXISTS t_sliced_desc_key;

CREATE TABLE t_sliced_desc_key (k UInt64, v UInt64)
ENGINE = MergeTree ORDER BY (k DESC)
SETTINGS index_granularity = 128, index_granularity_bytes = 10485760, add_minmax_index_for_numeric_columns = 0;

SYSTEM STOP MERGES t_sliced_desc_key;

-- Eight parts with disjoint key ranges of 20000 keys each.
INSERT INTO t_sliced_desc_key SELECT number, number * 7 FROM numbers(0, 20000);
INSERT INTO t_sliced_desc_key SELECT number, number * 7 FROM numbers(20000, 20000);
INSERT INTO t_sliced_desc_key SELECT number, number * 7 FROM numbers(40000, 20000);
INSERT INTO t_sliced_desc_key SELECT number, number * 7 FROM numbers(60000, 20000);
INSERT INTO t_sliced_desc_key SELECT number, number * 7 FROM numbers(80000, 20000);
INSERT INTO t_sliced_desc_key SELECT number, number * 7 FROM numbers(100000, 20000);
INSERT INTO t_sliced_desc_key SELECT number, number * 7 FROM numbers(120000, 20000);
INSERT INTO t_sliced_desc_key SELECT number, number * 7 FROM numbers(140000, 20000);

SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 't_sliced_desc_key' AND active;
SELECT 'router in pipeline', countIf(explain LIKE '%MergeTreeInOrderSliceRouter%') > 0
FROM (EXPLAIN PIPELINE SELECT k FROM t_sliced_desc_key ORDER BY k DESC LIMIT 1);
SELECT 'router in pipeline, reverse', countIf(explain LIKE '%MergeTreeInOrderSliceRouter%') > 0
FROM (EXPLAIN PIPELINE SELECT k FROM t_sliced_desc_key ORDER BY k ASC LIMIT 1);

-- Descending by k is the order of the key: the first part holds the keys 140000..159999, and the one
-- matching row is its last row in that order, so every slice of the part is read before the merge is
-- done, and no other part is needed.
SELECT 'last row of the first part', k FROM t_sliced_desc_key PREWHERE v = 140000 * 7 ORDER BY k DESC LIMIT 1 SETTINGS log_comment = '05616_desc';
SELECT 'last row of the first part', k FROM t_sliced_desc_key PREWHERE v = 140000 * 7 ORDER BY k DESC LIMIT 1 SETTINGS read_in_order_use_sliced_pool = 0;

-- Ascending by k reads the key in reverse: the first part is the one holding the keys 0..19999, and the
-- matching row is its last row in reading order.
SELECT 'last row of the first part, reverse', k FROM t_sliced_desc_key PREWHERE v = 19999 * 7 ORDER BY k ASC LIMIT 1 SETTINGS log_comment = '05616_asc';
SELECT 'last row of the first part, reverse', k FROM t_sliced_desc_key PREWHERE v = 19999 * 7 ORDER BY k ASC LIMIT 1 SETTINGS read_in_order_use_sliced_pool = 0;

SYSTEM FLUSH LOGS query_log;

-- Rows read: the first part (20000 rows) at most.
SELECT log_comment, read_rows <= 20000
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05616_desc', '05616_asc')
ORDER BY log_comment;

DROP TABLE t_sliced_desc_key;
