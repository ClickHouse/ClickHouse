-- When Cascades distributes the plan and `distributed_plan_join_runtime_filters` sends join runtime filters between
-- stages, the initiator places the filters, so that a stage that builds a filter can send it to the stages that apply
-- it. Without the transport the initiator places no filters; see 05366. A `RIGHT JOIN` is not broadcast, so the join
-- shuffles both sides, and the filter is applied before the shuffle of the probe side, in another stage than the build:
-- the filter is sent there. Without `optimize_move_to_prewhere` the filter is a step of its own, which Cascades could
-- put above the exchange.
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET param__internal_cascades_cluster_node_count = 4;
SET param__internal_cascades_cost_config = '{"sequential_weight":32,"network_weight":1,"exchange_fixed_overhead":1,"work_weight":1}';
SET enable_join_runtime_filters = 1;
SET join_runtime_filter_min_probe_rows = 0;
SET distributed_plan_join_runtime_filters = 1;
SET max_rows_to_group_by = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_rf_stage_fact;
DROP TABLE IF EXISTS t_rf_stage_dim;
CREATE TABLE t_rf_stage_fact (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY v;
CREATE TABLE t_rf_stage_dim (k UInt64, name String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_rf_stage_fact SELECT number % 1000, number FROM numbers(100000);
INSERT INTO t_rf_stage_dim SELECT number, if(number < 10, 'keep', 'skip') FROM numbers(1000);

SELECT '-- the initiator plan builds and applies the filter';
SELECT countIf(explain LIKE '%JoinLogical (Shuffle%') > 0, countIf(explain LIKE '%BuildRuntimeFilter%') > 0,
    countIf(explain LIKE '%Apply runtime join filter%') > 0
FROM (
    EXPLAIN SELECT sum(f.v) FROM t_rf_stage_fact AS f RIGHT JOIN t_rf_stage_dim AS d ON f.k = d.k WHERE d.name = 'keep'
);

SELECT '-- without PREWHERE the filter step is below the exchange of the probe side';
SELECT arrayFirstIndex(x -> x LIKE 'Filter ((Apply runtime join filter%', lines) > arrayFirstIndex(x -> x LIKE 'ShuffleExchange%', lines)
FROM (
    SELECT groupArray(trimLeft(explain)) AS lines FROM (
        EXPLAIN SELECT sum(f.v) FROM t_rf_stage_fact AS f RIGHT JOIN t_rf_stage_dim AS d ON f.k = d.k WHERE d.name = 'keep'
        SETTINGS optimize_move_to_prewhere = 0
    )
);

SELECT '-- the filter crosses stages, and the result is right';
SELECT sum(f.v) FROM t_rf_stage_fact AS f RIGHT JOIN t_rf_stage_dim AS d ON f.k = d.k WHERE d.name = 'keep'
SETTINGS log_comment = '05401_cascades_transport';
SELECT sum(f.v) FROM t_rf_stage_fact AS f RIGHT JOIN t_rf_stage_dim AS d ON f.k = d.k WHERE d.name = 'keep'
SETTINGS log_comment = '05401_cascades_transport_no_prewhere', optimize_move_to_prewhere = 0;
SELECT sum(f.v) FROM t_rf_stage_fact AS f RIGHT JOIN t_rf_stage_dim AS d ON f.k = d.k WHERE d.name = 'keep'
SETTINGS make_distributed_plan = 0;

SYSTEM FLUSH LOGS query_log;
SELECT q.log_comment, sum(t.ProfileEvents['RuntimeFilterStatesSent']) > 0
FROM system.query_log AS q
INNER JOIN system.query_log AS t ON t.initial_query_id = q.query_id
WHERE q.event_date >= yesterday() AND q.type = 'QueryFinish' AND q.is_initial_query AND q.current_database = currentDatabase()
    AND q.log_comment IN ('05401_cascades_transport', '05401_cascades_transport_no_prewhere')
    AND t.event_date >= yesterday() AND t.type = 'QueryFinish' AND t.query LIKE 'stage_%'
GROUP BY q.log_comment
ORDER BY q.log_comment
-- A distributed read of a system table can lose a part that a merge replaces in the meantime.
SETTINGS make_distributed_plan = 0;

DROP TABLE t_rf_stage_fact;
DROP TABLE t_rf_stage_dim;
