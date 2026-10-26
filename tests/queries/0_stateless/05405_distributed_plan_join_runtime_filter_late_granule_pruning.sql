-- A join runtime filter sent from another task can arrive after the probe side read has started. The index result of
-- a part is built when the first read task of the part starts, so the granules of that part are pruned again when a
-- later read task starts and the filter is ready. Here the build side is slow, so that the first read tasks start
-- before the filter exists, and the read tasks are small and slow, so that most of them start after it arrives. The
-- delays are in `PREWHERE`, which stays in the read and runs before the other conditions, so the reads are slow in any plan.
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET param__internal_cascades_cluster_node_count = 4;
SET param__internal_cascades_cost_config = '{"sequential_weight":32,"network_weight":1,"exchange_fixed_overhead":1,"work_weight":1}';
SET enable_join_runtime_filters = 1;
SET enable_join_runtime_filters_index_analysis = 1;
SET join_runtime_filter_min_probe_rows = 0;
SET distributed_plan_join_runtime_filters = 1;
SET max_rows_to_group_by = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'false';

DROP TABLE IF EXISTS t_rf_late_fact;
DROP TABLE IF EXISTS t_rf_late_dim;
CREATE TABLE t_rf_late_fact (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 100;
CREATE TABLE t_rf_late_dim (k UInt64, name String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_rf_late_fact SELECT number % 2000, number FROM numbers(20000);
INSERT INTO t_rf_late_dim SELECT number, if(number < 10, 'keep', 'skip') FROM numbers(2000);

SELECT '-- the result is right';
SELECT sum(f.v)
FROM (SELECT k, v FROM t_rf_late_fact PREWHERE sleepEachRow(0.0004) = 0) AS f
RIGHT JOIN (SELECT k, name FROM t_rf_late_dim PREWHERE sleepEachRow(0.00005) = 0) AS d ON f.k = d.k
WHERE d.name = 'keep'
-- The granules are pruned again when a read task starts, so the tasks are kept small also in the prefetching read pool
-- of a remote disk, where the task size follows its own settings. The columns cache is off: it aligns the tasks to its
-- stripes of up to 256 granules, so one task would read the whole part.
SETTINGS log_comment = '05405_late', max_threads = 2, max_block_size = 100, use_columns_cache = 0,
    merge_tree_min_rows_for_concurrent_read = 100, merge_tree_min_bytes_for_concurrent_read = 1,
    filesystem_prefetch_step_marks = 1, merge_tree_min_bytes_per_task_for_remote_reading = 1, merge_tree_min_read_task_size = 1,
    merge_tree_min_rows_for_concurrent_read_for_remote_filesystem = 0, merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem = 0;
SELECT sum(f.v) FROM t_rf_late_fact AS f RIGHT JOIN t_rf_late_dim AS d ON f.k = d.k WHERE d.name = 'keep'
SETTINGS make_distributed_plan = 0;

SELECT '-- the filter was sent, and the tasks prune granules with it';
SYSTEM FLUSH LOGS query_log;
SELECT sum(ProfileEvents['RuntimeFilterStatesSent']) > 0, sum(ProfileEvents['RuntimeFilterGranulesDropped']) > 0
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish'
    AND initial_query_id IN (
        SELECT query_id FROM system.query_log
        WHERE event_date >= yesterday() AND type = 'QueryFinish' AND is_initial_query
            AND current_database = currentDatabase() AND log_comment = '05405_late')
-- A distributed read of a system table can lose a part that a merge replaces in the meantime.
SETTINGS make_distributed_plan = 0;

DROP TABLE t_rf_late_fact;
DROP TABLE t_rf_late_dim;
