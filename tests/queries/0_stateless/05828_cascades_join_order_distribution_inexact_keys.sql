-- With `query_plan_join_order_distribution`, DPsub costs no shuffle join by an equality that the JOIN does not compare
-- exactly, such as `FixedString` = `String`: Cascades does not shuffle by it, because `'a'` and `'a\0\0'` are equal but
-- hash differently. Without this, the search would join `i_f` and `i_a` first by a shuffle that Cascades cannot use.

SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET distributed_plan_fallback_to_local_execution = 0;
-- The distribution-aware search runs only with the default `join_algorithm`.
SET join_algorithm = 'direct,hash,ie_join';
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET enable_join_transitive_predicates = 1;
SET query_plan_optimize_join_order_algorithm = 'dpsub';
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET use_hash_table_stats_for_join_reordering = 0;
SET join_use_nulls = 0;
SET explain_query_plan_default = 'legacy';
-- The test profile of CI sets these limits, which keep aggregations and joins on one node.
SET max_rows_to_group_by = 0;
SET max_rows_in_join = 0;
SET max_bytes_in_join = 0;
SET param__internal_cascades_cluster_node_count = 3;
-- A large cost of a hash table makes the broadcast joins too expensive, so the joins shuffle.
SET param__internal_cascades_cost_config = '{"hash_table_build_factor":1000000}';

CREATE TABLE i_f (s FixedString(3), j Int32) ENGINE = MergeTree ORDER BY s SETTINGS auto_statistics_types = '';
CREATE TABLE i_a (s String) ENGINE = MergeTree ORDER BY s SETTINGS auto_statistics_types = '';
CREATE TABLE i_b (j Int32) ENGINE = MergeTree ORDER BY j SETTINGS auto_statistics_types = '';
INSERT INTO i_f SELECT toString(number), number % 3 FROM numbers(10);
INSERT INTO i_a SELECT toString(number) FROM numbers(5);
INSERT INTO i_a SELECT toString(number) || '\0' FROM numbers(3);
INSERT INTO i_b SELECT number % 2 FROM numbers(4);

SET param__internal_join_table_stat_hints = '{
    "i_f": { "cardinality": 1000000, "avg_row_bytes": 7, "distinct_keys": { "s": 1000000, "j": 1000 } },
    "i_a": { "cardinality": 1000000, "avg_row_bytes": 4, "distinct_keys": { "s": 1000000 } },
    "i_b": { "cardinality": 1000000, "avg_row_bytes": 4, "distinct_keys": { "j": 1000 } }
}';

-- `i_f` and `i_b` are joined first, by a shuffle on `j`; `i_a` joins the result without a shuffle by `s`.
EXPLAIN
SELECT sum(i_f.j)
FROM i_f JOIN i_a ON i_f.s = i_a.s JOIN i_b ON i_f.j = i_b.j
SETTINGS query_plan_join_order_distribution = 1;

SELECT sum(i_f.j), count()
FROM i_f JOIN i_a ON i_f.s = i_a.s JOIN i_b ON i_f.j = i_b.j
SETTINGS query_plan_join_order_distribution = 1;

SELECT sum(i_f.j), count()
FROM i_f JOIN i_a ON i_f.s = i_a.s JOIN i_b ON i_f.j = i_b.j
SETTINGS query_plan_join_order_distribution = 0;
