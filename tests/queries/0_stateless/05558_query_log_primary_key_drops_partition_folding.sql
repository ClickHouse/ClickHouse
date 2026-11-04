-- Tags: no-parallel-replicas
-- no-parallel-replicas: index analysis runs on the replicas, the initiator does not see it

-- with constant folding the partition value goes only into the key condition of each part, its drops count too

SET use_query_condition_cache = 0;
SET use_statistics_for_part_pruning = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET distributed_index_analysis = 0;
SET max_rows_to_read = 0;
SET use_constant_folding_in_index_analysis = 1;

DROP TABLE IF EXISTS t_pk_folding;

CREATE TABLE t_pk_folding (p UInt64, k UInt64)
ENGINE = MergeTree
PARTITION BY p
ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;

-- one part of eight granules
INSERT INTO t_pk_folding SELECT 5, number FROM numbers(64);

SELECT k FROM t_pk_folding WHERE k = 5 SETTINGS log_comment = 'pk_folding_1' FORMAT Null;
SELECT k FROM t_pk_folding WHERE k = p SETTINGS log_comment = 'pk_folding_2' FORMAT Null;
SELECT k FROM t_pk_folding WHERE k = _partition_value.1 SETTINGS log_comment = 'pk_folding_3' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, read_rows, ProfileEvents['GranulesDroppedByPrimaryKey']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'pk_folding_%'
ORDER BY log_comment;

DROP TABLE t_pk_folding;
