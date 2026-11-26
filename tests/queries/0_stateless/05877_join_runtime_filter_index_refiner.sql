-- With `use_indexes_refiner_in_read_pools`, the read pool drops the granules a JOIN runtime filter
-- prunes once the filter is built. The results must not change, and on a point lookup the pool must
-- drop almost every mark instead of cutting tasks for them.

SET enable_join_runtime_filters = 1;
SET enable_join_runtime_filters_index_analysis = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_indexes_refiner_in_read_pools = 1;
-- The probe side builds no filter below the randomized threshold, and the asserted counters depend on
-- which side builds it.
SET join_runtime_filter_min_probe_rows = 0;
SET query_plan_join_swap_table = 'false';
SET query_plan_optimize_join_order_randomize = 0;
SET enable_join_seal_gated_reading = 0;
SET enable_parallel_replicas = 0;
-- A query condition cache hit would prune the granules before the pool sees them.
SET use_query_condition_cache = 0;
-- Two streams cutting small tasks: 10 marks on a local disk, `merge_tree_min_read_task_size` marks on a
-- remote one, where the task size otherwise follows `merge_tree_min_bytes_per_task_for_remote_reading`.
SET max_threads = 2;
SET allow_prefetched_read_pool_for_local_filesystem = 0;
SET allow_prefetched_read_pool_for_remote_filesystem = 0;
SET merge_tree_use_const_size_tasks_for_remote_reading = 1;
SET merge_tree_min_rows_for_concurrent_read = 640;
SET merge_tree_min_bytes_for_concurrent_read = 0;
SET merge_tree_min_bytes_per_task_for_remote_reading = 1;
-- The columns cache aligns the cuts to its stripes of up to 256 marks.
SET use_columns_cache = 0;
SET merge_tree_read_end_tasks_per_stream = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

DROP TABLE IF EXISTS t_rf_refiner;

CREATE TABLE t_rf_refiner (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 64, index_granularity_bytes = '10Mi';

INSERT INTO t_rf_refiner SELECT number, number FROM numbers(200000);
OPTIMIZE TABLE t_rf_refiner FINAL;

SELECT 'INNER', count(), sum(t.v) FROM t_rf_refiner AS t INNER JOIN (SELECT toUInt64(150000) AS k) AS b ON t.k = b.k
SETTINGS log_comment = '05877_inner_refiner_on';
SELECT 'LEFT SEMI', count(), sum(t.v) FROM t_rf_refiner AS t LEFT SEMI JOIN (SELECT toUInt64(150000) AS k) AS b ON t.k = b.k;
SELECT 'RIGHT', count(), sum(t.v) FROM t_rf_refiner AS t RIGHT JOIN (SELECT toUInt64(150000) AS k) AS b ON t.k = b.k;
SELECT 'LEFT ANTI', count(), sum(t.v) FROM t_rf_refiner AS t LEFT ANTI JOIN (SELECT toUInt64(150000) AS k) AS b ON t.k = b.k;

SELECT 'INNER prefetched', count(), sum(t.v) FROM t_rf_refiner AS t INNER JOIN (SELECT toUInt64(150000) AS k) AS b ON t.k = b.k
SETTINGS allow_prefetched_read_pool_for_local_filesystem = 1, local_filesystem_read_method = 'pread_threadpool', log_comment = '05877_inner_prefetched_refiner_on';
SELECT 'LEFT SEMI prefetched', count(), sum(t.v) FROM t_rf_refiner AS t LEFT SEMI JOIN (SELECT toUInt64(150000) AS k) AS b ON t.k = b.k
SETTINGS allow_prefetched_read_pool_for_local_filesystem = 1, local_filesystem_read_method = 'pread_threadpool';
SELECT 'RIGHT prefetched', count(), sum(t.v) FROM t_rf_refiner AS t RIGHT JOIN (SELECT toUInt64(150000) AS k) AS b ON t.k = b.k
SETTINGS allow_prefetched_read_pool_for_local_filesystem = 1, local_filesystem_read_method = 'pread_threadpool';
SELECT 'LEFT ANTI prefetched', count(), sum(t.v) FROM t_rf_refiner AS t LEFT ANTI JOIN (SELECT toUInt64(150000) AS k) AS b ON t.k = b.k
SETTINGS allow_prefetched_read_pool_for_local_filesystem = 1, local_filesystem_read_method = 'pread_threadpool';

SELECT count(), sum(t.v) FROM t_rf_refiner AS t INNER JOIN (SELECT toUInt64(150000) AS k) AS b ON t.k = b.k
FORMAT Null
SETTINGS log_comment = '05877_inner_refiner_off', use_indexes_refiner_in_read_pools = 0;

SYSTEM FLUSH LOGS query_log;

-- 3125 marks, one of them survives the filter.
SELECT
    argMax(ProfileEvents['RuntimeFilterGranulesDropped'], event_time_microseconds) > 2500,
    argMax(ProfileEvents['ReadPoolRangeRefinerDroppedMarks'], event_time_microseconds) > 2500
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05877_inner_refiner_on' AND type = 'QueryFinish';

SELECT
    argMax(ProfileEvents['RuntimeFilterGranulesDropped'], event_time_microseconds) > 2500,
    argMax(ProfileEvents['ReadPoolRangeRefinerDroppedMarks'], event_time_microseconds) = 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05877_inner_refiner_off' AND type = 'QueryFinish';

SELECT
    argMax(ProfileEvents['RuntimeFilterGranulesDropped'], event_time_microseconds) > 2500,
    argMax(ProfileEvents['ReadPoolRangeRefinerDroppedMarks'], event_time_microseconds) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05877_inner_prefetched_refiner_on' AND type = 'QueryFinish';

DROP TABLE t_rf_refiner;
