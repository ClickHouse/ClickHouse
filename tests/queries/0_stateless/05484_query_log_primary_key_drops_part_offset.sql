-- Tags: no-parallel-replicas
-- no-parallel-replicas: index analysis runs on the replicas, the initiator does not see it

-- marks that `_part_offset` conditions exclude are not primary key drops

SET use_query_condition_cache = 0;
SET use_statistics_for_part_pruning = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET distributed_index_analysis = 0;
SET max_rows_to_read = 0;

DROP TABLE IF EXISTS t_pk_offset;
DROP TABLE IF EXISTS t_no_pk_offset;

CREATE TABLE t_pk_offset (k UInt64)
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = 0, add_minmax_index_for_numeric_columns = 0;

CREATE TABLE t_no_pk_offset (k UInt64)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8, index_granularity_bytes = 0, add_minmax_index_for_numeric_columns = 0;

-- one part of eight granules
INSERT INTO t_pk_offset SELECT number FROM numbers(64);
INSERT INTO t_no_pk_offset SELECT number FROM numbers(64);

SELECT k FROM t_pk_offset WHERE _part_offset = 5 SETTINGS use_primary_key = 0, log_comment = 'pk_offset_1' FORMAT Null;
SELECT k FROM t_pk_offset WHERE _part_offset = 5 SETTINGS log_comment = 'pk_offset_2' FORMAT Null;
SELECT k FROM t_no_pk_offset WHERE _part_offset = 5 SETTINGS log_comment = 'pk_offset_3' FORMAT Null;
SELECT k FROM t_pk_offset WHERE k > 31 SETTINGS log_comment = 'pk_offset_4' FORMAT Null;
SELECT k FROM t_pk_offset WHERE k > 31 AND _part_offset = 40 SETTINGS log_comment = 'pk_offset_5' FORMAT Null;
SELECT k FROM t_pk_offset WHERE k > 31 AND _part_starting_offset + _part_offset = 40 SETTINGS log_comment = 'pk_offset_6' FORMAT Null;
SELECT k FROM t_pk_offset WHERE k > 31 AND _part_offset = 40 SETTINGS use_primary_key = 0, log_comment = 'pk_offset_7' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, read_rows, ProfileEvents['GranulesDroppedByPrimaryKey']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'pk_offset_%'
ORDER BY log_comment;

DROP TABLE t_pk_offset;
DROP TABLE t_no_pk_offset;
