-- Tags: no-parallel-replicas: index analysis runs on the replicas, the initiator does not see it

SET enable_join_runtime_filters = 1;
SET join_runtime_filter_min_probe_rows = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'false';
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
-- a throwing `max_rows_to_read` turns off skip indexes on data read, the test profile sets it
SET max_rows_to_read = 0;
SET use_query_condition_cache = 0;
SET use_statistics_for_part_pruning = 0;

DROP TABLE IF EXISTS t_skip_index_runtime_fact;
DROP TABLE IF EXISTS t_skip_index_runtime_dim;

-- the index is on a column that is not in the primary key
CREATE TABLE t_skip_index_runtime_fact (k UInt64, id UInt64, INDEX idx_id id TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 16, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE t_skip_index_runtime_dim (id UInt64, tag String) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_skip_index_runtime_fact SELECT number, number FROM numbers(100000);
INSERT INTO t_skip_index_runtime_dim SELECT number, if(number < 64, 'hot', 'cold') FROM numbers(1000);

-- only the join runtime filter uses idx_id
SELECT count() FROM t_skip_index_runtime_fact AS f INNER JOIN t_skip_index_runtime_dim AS d ON f.id = d.id WHERE d.tag = 'hot'
SETTINGS enable_join_runtime_filters_index_analysis = 1, log_comment = 'skip_index_runtime_1';
SELECT count() FROM t_skip_index_runtime_fact AS f INNER JOIN t_skip_index_runtime_dim AS d ON f.id = d.id WHERE d.tag = 'hot'
SETTINGS enable_join_runtime_filters_index_analysis = 0, log_comment = 'skip_index_runtime_2';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    arrayMap(x -> replaceOne(x, currentDatabase() || '.', ''), used_data_skipping_indices),
    ProfileEvents['RuntimeFilterGranulesDropped'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'skip_index_runtime_%'
ORDER BY log_comment;

DROP TABLE t_skip_index_runtime_fact;
DROP TABLE t_skip_index_runtime_dim;
