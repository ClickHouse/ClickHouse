-- Tags: no-parallel-replicas
-- no-parallel-replicas: the query condition cache is populated per replica

-- The `__topKFilter` PREWHERE of an `ORDER BY ... LIMIT` read drops granules based on a threshold
-- derived from the rows of all parts. While a `DROP COLUMN` followed by `ADD COLUMN` of the same name,
-- or a `RENAME COLUMN`, is pending for one part, the values read from that part tighten the threshold
-- for the other parts too. Granules of another part must not be recorded as empty under such a
-- threshold: after `KILL MUTATION` the first part returns other values and the skipped granules may
-- hold the top rows. See also `05289_query_condition_cache_topk_on_fly_mutations`.

SET use_query_condition_cache = 1;
SET use_query_condition_cache_for_top_k = 1;
SET use_top_k_dynamic_filtering = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET max_threads = 1;
SET max_block_size = 8192;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET merge_tree_min_rows_for_seek = 0;
SET merge_tree_min_bytes_for_seek = 0;
SET mutations_sync = 0;
SET alter_sync = 0;

SELECT 'DROP COLUMN and ADD COLUMN';

DROP TABLE IF EXISTS t_qcc_topk_readd;
CREATE TABLE t_qcc_topk_readd (id UInt32, k UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, add_minmax_index_for_numeric_columns = 0;

SYSTEM STOP MERGES t_qcc_topk_readd;
INSERT INTO t_qcc_topk_readd SELECT number, 1000000 + number FROM numbers(8192);
-- Stays pending, so the first part reads `k` as the default of the added column.
ALTER TABLE t_qcc_topk_readd DROP COLUMN k, ADD COLUMN k UInt32;
-- Inserted after the mutation, which therefore does not apply to this part.
INSERT INTO t_qcc_topk_readd SELECT 100000 + number, 1000 + number FROM numbers(100000);

SELECT k FROM t_qcc_topk_readd ORDER BY k LIMIT 5;

SELECT 'after KILL MUTATION';
KILL MUTATION WHERE database = currentDatabase() AND table = 't_qcc_topk_readd' SYNC FORMAT Null;
SELECT k FROM t_qcc_topk_readd ORDER BY k LIMIT 5;
SELECT k FROM t_qcc_topk_readd ORDER BY k LIMIT 5 SETTINGS use_query_condition_cache = 0;

DROP TABLE t_qcc_topk_readd;

SELECT 'RENAME COLUMN';

DROP TABLE IF EXISTS t_qcc_topk_rename;
CREATE TABLE t_qcc_topk_rename (id UInt32, k UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, add_minmax_index_for_numeric_columns = 0;

SYSTEM STOP MERGES t_qcc_topk_rename;
INSERT INTO t_qcc_topk_rename SELECT number, 1000000 + number FROM numbers(8192);
-- Stays pending, so the first part reads `c` from its column `k`.
ALTER TABLE t_qcc_topk_rename RENAME COLUMN k TO c;
-- Inserted after the mutation, which therefore does not apply to this part.
INSERT INTO t_qcc_topk_rename SELECT 100000 + number, 1000 + number FROM numbers(100000);

SELECT c FROM t_qcc_topk_rename ORDER BY c DESC LIMIT 5;

SELECT 'after KILL MUTATION';
KILL MUTATION WHERE database = currentDatabase() AND table = 't_qcc_topk_rename' SYNC FORMAT Null;
SELECT c FROM t_qcc_topk_rename ORDER BY c DESC LIMIT 5;
SELECT c FROM t_qcc_topk_rename ORDER BY c DESC LIMIT 5 SETTINGS use_query_condition_cache = 0;

DROP TABLE t_qcc_topk_rename;
