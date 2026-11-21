-- With `use_skip_indexes_if_final = 0`, the replicas of distributed index analysis must not prune a FINAL read by a skip index

SET allow_experimental_parallel_reading_from_replicas = 0;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_two_replicas';
SET max_parallel_replicas = 2;
SET use_skip_indexes = 1;
SET explain_query_plan_default = 'legacy';
-- Ignore `Cannot connect to {}. It will not participate in distributed index analysis`
SET send_logs_level = 'error';

DROP TABLE IF EXISTS t_dia_final;
DROP TABLE IF EXISTS t_dia_final_pk;

-- `toYYYYMM(d)` in the primary key makes the filter on `d` reach the replicas, where `idx_d` can use it
CREATE TABLE t_dia_final (k UInt64, d Date, ver UInt64, INDEX idx_d d TYPE minmax GRANULARITY 1)
ENGINE = ReplacingMergeTree(ver) ORDER BY (toYYYYMM(d), k)
SETTINGS distributed_index_analysis_min_parts_to_activate = 1, distributed_index_analysis_min_indexes_bytes_to_activate = 0;

SYSTEM STOP MERGES t_dia_final;

-- one part per row: version 1 of every key on 2026-01-01, version 2 of the even keys on 2026-01-02
INSERT INTO t_dia_final SELECT number, '2026-01-01', 1 FROM numbers(20) SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, max_insert_threads = 1, async_insert = 0;
INSERT INTO t_dia_final SELECT number, '2026-01-02', 2 FROM numbers(20) WHERE number % 2 = 0 SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, max_insert_threads = 1, async_insert = 0;

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_dia_final' AND active;

SELECT count() FROM t_dia_final FINAL WHERE d = '2026-01-01' SETTINGS use_skip_indexes_if_final = 0, distributed_index_analysis = 0;
SELECT count() FROM t_dia_final FINAL WHERE d = '2026-01-01' SETTINGS use_skip_indexes_if_final = 0, distributed_index_analysis = 1, distributed_index_analysis_for_non_shared_merge_tree = 1;
SELECT count() FROM t_dia_final WHERE d = '2026-01-01' SETTINGS final = 1, use_skip_indexes_if_final = 0, distributed_index_analysis = 1, distributed_index_analysis_for_non_shared_merge_tree = 1;

-- `Distributed:` in EXPLAIN = the read used distributed index analysis: not with the skip index, but with `use_skip_indexes = 0` or without a skip index
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dia_final FINAL WHERE d = '2026-01-01' SETTINGS use_skip_indexes_if_final = 0, distributed_index_analysis = 1, distributed_index_analysis_for_non_shared_merge_tree = 1) WHERE explain LIKE '%Distributed:%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dia_final FINAL WHERE d = '2026-01-01' SETTINGS use_skip_indexes_if_final = 0, use_skip_indexes = 0, distributed_index_analysis = 1, distributed_index_analysis_for_non_shared_merge_tree = 1) WHERE explain LIKE '%Distributed:%';

CREATE TABLE t_dia_final_pk (k UInt64, d Date, ver UInt64)
ENGINE = ReplacingMergeTree(ver) ORDER BY (toYYYYMM(d), k)
SETTINGS distributed_index_analysis_min_parts_to_activate = 1, distributed_index_analysis_min_indexes_bytes_to_activate = 0;
INSERT INTO t_dia_final_pk SELECT * FROM t_dia_final;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_dia_final_pk FINAL WHERE d = '2026-01-01' SETTINGS use_skip_indexes_if_final = 0, distributed_index_analysis = 1, distributed_index_analysis_for_non_shared_merge_tree = 1) WHERE explain LIKE '%Distributed:%';

DROP TABLE t_dia_final;
DROP TABLE t_dia_final_pk;
