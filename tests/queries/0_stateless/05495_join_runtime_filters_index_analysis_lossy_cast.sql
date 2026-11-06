-- A join key under a cast that can change values (e.g. narrowing) must not be used to prune granules by its
-- uncast values: `CAST(300 AS UInt8)` is 44, so the row 300 matches 44 and its granule has to be read.

SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1;
SET join_runtime_filter_min_probe_rows = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET enable_join_runtime_filters_index_analysis = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_join_swap_table = 'false';
SET use_query_condition_cache = 0;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_rf_lossy_cast;
CREATE TABLE t_rf_lossy_cast (id UInt32)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 16, index_granularity_bytes = '10Mi';
SYSTEM STOP MERGES t_rf_lossy_cast;
INSERT INTO t_rf_lossy_cast SELECT number FROM numbers(512);

-- Narrowing cast of the global row index: exact values and range.
SELECT a.id FROM t_rf_lossy_cast AS a, (SELECT toUInt64(44) AS x) AS b
WHERE CAST(a._part_starting_offset + a._part_offset AS UInt8) = b.x ORDER BY a.id
SETTINGS log_comment = '05495_global_row_index_narrowing';

SELECT a.id FROM t_rf_lossy_cast AS a, (SELECT toUInt64(44) AS x) AS b
WHERE CAST(a._part_starting_offset + a._part_offset AS UInt8) = b.x ORDER BY a.id
SETTINGS join_runtime_filter_exact_values_limit = 0, log_comment = '05495_global_row_index_narrowing_range';

-- Narrowing cast of a primary key column: exact values and range.
SELECT a.id FROM t_rf_lossy_cast AS a, (SELECT toUInt64(44) AS x) AS b
WHERE CAST(a.id AS UInt8) = b.x ORDER BY a.id
SETTINGS log_comment = '05495_primary_key_narrowing';

SELECT a.id FROM t_rf_lossy_cast AS a, (SELECT toUInt64(44) AS x) AS b
WHERE CAST(a.id AS UInt8) = b.x ORDER BY a.id
SETTINGS join_runtime_filter_exact_values_limit = 0, log_comment = '05495_primary_key_narrowing_range';

-- A widening cast does not change values, so the key still prunes.
SELECT a.id FROM t_rf_lossy_cast AS a, (SELECT toUInt128(300) AS x) AS b
WHERE CAST(a._part_starting_offset + a._part_offset AS UInt128) = b.x ORDER BY a.id
SETTINGS log_comment = '05495_global_row_index_widening';

SELECT a.id FROM t_rf_lossy_cast AS a, (SELECT toUInt64(300) AS x) AS b
WHERE a.id = b.x ORDER BY a.id
SETTINGS log_comment = '05495_primary_key_widening';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    argMax(ProfileEvents['RuntimeFilterIndexAnalysisReads'], event_time) > 0,
    argMax(ProfileEvents['RuntimeFilterGranulesDropped'], event_time) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
    AND log_comment IN ('05495_global_row_index_narrowing', '05495_global_row_index_narrowing_range',
        '05495_primary_key_narrowing', '05495_primary_key_narrowing_range', '05495_global_row_index_widening',
        '05495_primary_key_widening')
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE t_rf_lossy_cast;
