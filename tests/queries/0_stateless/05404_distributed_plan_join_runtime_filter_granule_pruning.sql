-- With a distributed query plan, a task prunes the granules of its probe side read with the runtime filter of a
-- join in the same task. The broadcast join below reads the small side in every task and builds the filter there,
-- so the filter is ready before the task reads the probe side.
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
SET max_rows_to_group_by = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'false';
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_rf_prune_fact;
DROP TABLE IF EXISTS t_rf_prune_dim;
CREATE TABLE t_rf_prune_fact (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1000;
CREATE TABLE t_rf_prune_dim (k UInt64, name String) ENGINE = MergeTree ORDER BY k;
-- The broadcast join is chosen by the column statistics.
SET materialize_statistics_on_insert = 1;
SET use_statistics = 1;
INSERT INTO t_rf_prune_fact SELECT number % 20000, number FROM numbers(200000);
INSERT INTO t_rf_prune_dim SELECT number, if(number < 10, 'keep', 'skip') FROM numbers(20000);

SELECT '-- the join is a broadcast join';
SELECT countIf(explain LIKE '%JoinLogical (Broadcast%') > 0
FROM (EXPLAIN SELECT sum(f.v) FROM t_rf_prune_fact AS f JOIN t_rf_prune_dim AS d ON f.k = d.k WHERE d.name = 'keep');

SELECT '-- the result is right';
SELECT sum(f.v) FROM t_rf_prune_fact AS f JOIN t_rf_prune_dim AS d ON f.k = d.k WHERE d.name = 'keep'
SETTINGS log_comment = '05404_distributed';
SELECT sum(f.v) FROM t_rf_prune_fact AS f JOIN t_rf_prune_dim AS d ON f.k = d.k WHERE d.name = 'keep'
SETTINGS make_distributed_plan = 0;

SELECT '-- the tasks prune granules';
SYSTEM FLUSH LOGS query_log;
SELECT sum(ProfileEvents['RuntimeFilterIndexAnalysisReads']) > 0, sum(ProfileEvents['RuntimeFilterGranulesDropped']) > 0
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish'
    AND initial_query_id IN (
        SELECT query_id FROM system.query_log
        WHERE event_date >= yesterday() AND type = 'QueryFinish' AND is_initial_query
            AND current_database = currentDatabase() AND log_comment = '05404_distributed')
-- A distributed read of a system table can lose a part that a merge replaces in the meantime.
SETTINGS make_distributed_plan = 0;

DROP TABLE t_rf_prune_fact;
DROP TABLE t_rf_prune_dim;
