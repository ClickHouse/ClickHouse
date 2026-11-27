-- The global row index `_part_starting_offset + _part_offset` under casts: a single cast to the type of the runtime filter
-- is applied again in the pruning predicate, so it prunes; nested casts, e.g. a narrowing `CAST` compared with a wider
-- key, are not used for pruning, since `CAST(300 AS UInt8)` is 44 and the row 300 must be read.

SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1;
SET join_runtime_filter_min_probe_rows = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET enable_join_runtime_filters_index_analysis = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_join_swap_table = 'false';
SET use_query_condition_cache = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_rf_global_row_index_cast;
CREATE TABLE t_rf_global_row_index_cast (id UInt32)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 16, index_granularity_bytes = '10Mi';
SYSTEM STOP MERGES t_rf_global_row_index_cast;
INSERT INTO t_rf_global_row_index_cast SELECT number FROM numbers(512);

-- Nested casts: exact values and range.
SELECT a.id FROM t_rf_global_row_index_cast AS a, (SELECT toUInt64(44) AS x) AS b
WHERE CAST(a._part_starting_offset + a._part_offset AS UInt8) = b.x ORDER BY a.id
SETTINGS log_comment = '05495_narrowing';

SELECT a.id FROM t_rf_global_row_index_cast AS a, (SELECT toUInt64(44) AS x) AS b
WHERE CAST(a._part_starting_offset + a._part_offset AS UInt8) = b.x ORDER BY a.id
SETTINGS join_runtime_filter_exact_values_limit = 0, log_comment = '05495_narrowing_range';

-- A single cast to the filter type.
SELECT a.id FROM t_rf_global_row_index_cast AS a, (SELECT toUInt128(300) AS x) AS b
WHERE a._part_starting_offset + a._part_offset = b.x ORDER BY a.id
SETTINGS log_comment = '05495_to_uint128';

SELECT a.id FROM t_rf_global_row_index_cast AS a, (SELECT toInt128(300) AS x) AS b
WHERE a._part_starting_offset + a._part_offset = b.x ORDER BY a.id
SETTINGS log_comment = '05495_to_int128';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    argMax(ProfileEvents['RuntimeFilterIndexAnalysisReads'], event_time_microseconds) > 0,
    argMax(ProfileEvents['RuntimeFilterGranulesDropped'], event_time_microseconds) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
    AND log_comment IN ('05495_narrowing', '05495_narrowing_range', '05495_to_uint128', '05495_to_int128')
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE t_rf_global_row_index_cast;
