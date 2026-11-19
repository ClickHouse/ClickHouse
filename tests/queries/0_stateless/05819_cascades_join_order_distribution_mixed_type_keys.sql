-- With `query_plan_join_order_distribution`, DPsub costs a shuffle join also by an equality of two columns of different
-- types. Such an equality is in no equivalence class. Without this, the search sees only a broadcast for the join of
-- `m_f` and `m_a`, and joins `m_f` with `m_b` first, which gives many more rows.

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

CREATE TABLE m_f (k UInt64, j Int32) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE m_a (k UInt32) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE m_b (j Int32) ENGINE = MergeTree ORDER BY j SETTINGS auto_statistics_types = '';
INSERT INTO m_f SELECT number, number % 3 FROM numbers(10);
INSERT INTO m_a SELECT number FROM numbers(5);
INSERT INTO m_b SELECT number % 2 FROM numbers(4);

SET param__internal_join_table_stat_hints = '{
    "m_f": { "cardinality": 1000000, "avg_row_bytes": 12, "distinct_keys": { "k": 1000000, "j": 1000 } },
    "m_a": { "cardinality": 1000000, "avg_row_bytes": 4, "distinct_keys": { "k": 1000000 } },
    "m_b": { "cardinality": 1000000, "avg_row_bytes": 4, "distinct_keys": { "j": 1000 } }
}';

-- `m_f` and `m_a` are joined first, by a shuffle on the `UInt64` = `UInt32` equality.
EXPLAIN
SELECT sum(m_f.j)
FROM m_f JOIN m_a ON m_f.k = m_a.k JOIN m_b ON m_f.j = m_b.j
SETTINGS query_plan_join_order_distribution = 1;

SELECT sum(m_f.k), count()
FROM m_f JOIN m_a ON m_f.k = m_a.k JOIN m_b ON m_f.j = m_b.j
SETTINGS query_plan_join_order_distribution = 1;

SELECT sum(m_f.k), count()
FROM m_f JOIN m_a ON m_f.k = m_a.k JOIN m_b ON m_f.j = m_b.j
SETTINGS query_plan_join_order_distribution = 0;
