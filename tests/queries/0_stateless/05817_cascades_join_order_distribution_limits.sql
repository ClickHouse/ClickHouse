-- `query_plan_join_order_distribution` on inputs that Cascades cannot distribute, and at the limits of the search.

SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET distributed_plan_fallback_to_local_execution = 0;
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
