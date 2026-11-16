-- `query_plan_join_order_distribution` on inputs that Cascades cannot distribute, and at the limits of the search.

SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET distributed_plan_fallback_to_local_execution = 0;
-- The distribution-aware search runs only with the default `join_algorithm`.
SET join_algorithm = 'direct,hash,ie_join';
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET enable_join_transitive_predicates = 1;
SET query_plan_join_order_distribution = 1;
SET query_plan_optimize_join_order_randomize = 0;
SET max_rows_to_group_by = 0;
SET max_rows_in_join = 0;
SET max_bytes_in_join = 0;
SET param__internal_cascades_cluster_node_count = 8;

CREATE TABLE j_a (a Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE j_b (b Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE j_c (c Int32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE j_d (d Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO j_a VALUES (1);
INSERT INTO j_b VALUES (2);
INSERT INTO j_c VALUES (1);
INSERT INTO j_d VALUES (1);

-- A `FULL JOIN ON 0` is an input of the inner joins: its condition is a constant, not an equality of two columns.
SELECT 'constant condition', count()
FROM (SELECT a, b FROM j_a FULL ALL JOIN j_b ON 0) AS u
INNER ALL JOIN j_c ON u.a = j_c.c
INNER ALL JOIN j_d ON j_c.c = j_d.d
SETTINGS query_plan_optimize_join_order_limit = 3, query_plan_optimize_join_order_conflict_detector = 'c',
    query_plan_convert_outer_join_to_inner_join = 0, join_use_nulls = 0;

-- Over the budget of pairs of plans, the graph is planned again without distribution: the plan is the one with the
-- setting off. Only `dpsub` may plan it, so the second search must give the plan.
SET explain_query_plan_default = 'legacy';
SET query_plan_join_swap_table = 'false';
SET use_hash_table_stats_for_join_reordering = 0;
CREATE TABLE q_a (a Int32, a_value Int32) ENGINE = MergeTree ORDER BY a SETTINGS auto_statistics_types = '';
CREATE TABLE q_b (b Int32, b_value Int32) ENGINE = MergeTree ORDER BY b SETTINGS auto_statistics_types = '';
CREATE TABLE q_c (c Int32, c_value Int32) ENGINE = MergeTree ORDER BY c SETTINGS auto_statistics_types = '';
CREATE TABLE q_d (d Int32, d_value Int32) ENGINE = MergeTree ORDER BY d SETTINGS auto_statistics_types = '';
INSERT INTO q_a SELECT number % 10, number FROM numbers(20);
INSERT INTO q_b SELECT number % 12, number FROM numbers(20);
INSERT INTO q_c SELECT number % 14, number FROM numbers(20);
INSERT INTO q_d SELECT number % 16, number FROM numbers(20);
SET param__internal_join_table_stat_hints = '{
    "q_a": { "cardinality": 10000000, "avg_row_bytes": 8, "distinct_keys": { "a": 10000000 } },
    "q_b": { "cardinality": 20000000, "avg_row_bytes": 8, "distinct_keys": { "b": 10000000 } },
    "q_c": { "cardinality": 30000000, "avg_row_bytes": 8, "distinct_keys": { "c": 10000000 } },
    "q_d": { "cardinality": 40000000, "avg_row_bytes": 8, "distinct_keys": { "d": 10000000 } }
}';

SET param__internal_dpsub_distribution_plan_pair_limit = 5;
SELECT 'over budget, plan of the setting off',
    (SELECT groupArray(explain) FROM (EXPLAIN
    SELECT sum(a_value + b_value + c_value + d_value)
    FROM q_a JOIN q_b ON a = b JOIN q_c ON b = c JOIN q_d ON a = d
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_limit = 10, make_distributed_plan = 1))
    = (SELECT groupArray(explain) FROM (EXPLAIN
    SELECT sum(a_value + b_value + c_value + d_value)
    FROM q_a JOIN q_b ON a = b JOIN q_c ON b = c JOIN q_d ON a = d
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_limit = 10, make_distributed_plan = 1,
        query_plan_join_order_distribution = 0))
SETTINGS make_distributed_plan = 0;
SELECT 'over budget', sum(a_value + b_value + c_value + d_value)
FROM q_a JOIN q_b ON a = b JOIN q_c ON b = c JOIN q_d ON a = d
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_limit = 10;
SET param__internal_dpsub_distribution_plan_pair_limit = 0;

-- With one partitioning state, the search finds two classes, `a`, `c`, `d` and `k`, `b`: it keeps the plans in the
-- state it cannot add as unspecified ones.
CREATE TABLE j_k (k Int32, kc Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO j_k VALUES (2, 1);
SET param__internal_dpsub_distribution_partition_state_limit = 1;
SELECT 'one state', count()
FROM j_a JOIN j_c ON a = c JOIN j_d ON c = d JOIN j_k ON c = kc JOIN j_b ON k = b
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', query_plan_optimize_join_order_limit = 10;
SET param__internal_dpsub_distribution_partition_state_limit = 0;
