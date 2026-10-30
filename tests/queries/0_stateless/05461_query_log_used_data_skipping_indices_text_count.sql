-- Tags: no-parallel-replicas: index analysis runs on the replicas, the initiator does not see it

SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
SET use_query_condition_cache = 0;
SET use_statistics_for_part_pruning = 0;
-- the text index count optimization needs it
SET optimize_trivial_count_query = 1;
SET query_plan_direct_read_from_text_index = 1;
SET optimize_use_implicit_projections = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET distributed_index_analysis = 0;

DROP TABLE IF EXISTS t_skip_index_text_count;

CREATE TABLE t_skip_index_text_count (k UInt64, s String)
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;

-- the first part has no index, the second part has it; eight granules each, one row matches in each part
INSERT INTO t_skip_index_text_count SELECT number, if(number = 5, 'needle', 'hay') FROM numbers(64);
ALTER TABLE t_skip_index_text_count ADD INDEX idx_s s TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1;
INSERT INTO t_skip_index_text_count SELECT number + 64, if(number = 5, 'needle', 'hay') FROM numbers(64);

-- the count step replaces the read of the indexed part only
SELECT count() FROM t_skip_index_text_count WHERE hasToken(s, 'needle') SETTINGS query_plan_optimize_count_from_text_index = 0, log_comment = 'skip_index_text_count_1';
SELECT count() FROM t_skip_index_text_count WHERE hasToken(s, 'needle') SETTINGS query_plan_optimize_count_from_text_index = 1, log_comment = 'skip_index_text_count_2';

-- the count step replaces the whole read
ALTER TABLE t_skip_index_text_count MATERIALIZE INDEX idx_s SETTINGS mutations_sync = 2;
SELECT count() FROM t_skip_index_text_count WHERE hasToken(s, 'needle') SETTINGS query_plan_optimize_count_from_text_index = 0, log_comment = 'skip_index_text_count_3';
SELECT count() FROM t_skip_index_text_count WHERE hasToken(s, 'needle') SETTINGS query_plan_optimize_count_from_text_index = 1, log_comment = 'skip_index_text_count_4';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    arrayMap(x -> replaceOne(x, currentDatabase() || '.', ''), used_data_skipping_indices),
    ProfileEvents['GranulesDroppedByPartitionKey'],
    ProfileEvents['GranulesDroppedByPrimaryKey'],
    ProfileEvents['GranulesDroppedBySkipIndexes']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'skip_index_text_count_%'
ORDER BY log_comment;

DROP TABLE t_skip_index_text_count;
