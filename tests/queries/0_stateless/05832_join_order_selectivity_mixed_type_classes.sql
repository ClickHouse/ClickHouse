-- The join order search must not estimate a join by an equality that no join condition checks.
-- `f.k` is `UInt64`, `a.k` and `b.k` are `UInt32`: the class of `k` has several types, so no `a.k = b.k` is added
-- to a join of `a` with `b`, and that join must be estimated as a cross product, not as a join on `k`.

CREATE TABLE f (k UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE a (k UInt32) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE b (k UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO f SELECT number FROM numbers(10000);
INSERT INTO a SELECT number FROM numbers(1000);
INSERT INTO b SELECT number FROM numbers(1000);

SET enable_analyzer = 1;

SELECT 'cross', (SELECT count() FROM a CROSS JOIN b JOIN f ON a.k = f.k AND b.k = f.k);
SELECT 'cross', 'dpsub', countIf(explain LIKE '%Type: cross%') FROM (EXPLAIN actions = 1 SELECT count() FROM a CROSS JOIN b JOIN f ON a.k = f.k AND b.k = f.k
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', enable_join_transitive_predicates = 1, query_plan_optimize_join_order_randomize = 0, use_statistics = 0, use_hash_table_stats_for_join_reordering = 0, enable_parallel_replicas = 0, query_plan_join_swap_table = 'auto', query_plan_optimize_join_order_limit = 10);
SELECT 'cross', 'greedy', countIf(explain LIKE '%Type: cross%') FROM (EXPLAIN actions = 1 SELECT count() FROM a CROSS JOIN b JOIN f ON a.k = f.k AND b.k = f.k
    SETTINGS query_plan_optimize_join_order_algorithm = 'greedy', enable_join_transitive_predicates = 1, query_plan_optimize_join_order_randomize = 0, use_statistics = 0, use_hash_table_stats_for_join_reordering = 0, enable_parallel_replicas = 0, query_plan_join_swap_table = 'auto', query_plan_optimize_join_order_limit = 10);
SELECT 'cross', 'dpsize', countIf(explain LIKE '%Type: cross%') FROM (EXPLAIN actions = 1 SELECT count() FROM a CROSS JOIN b JOIN f ON a.k = f.k AND b.k = f.k
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize', enable_join_transitive_predicates = 1, query_plan_optimize_join_order_randomize = 0, use_statistics = 0, use_hash_table_stats_for_join_reordering = 0, enable_parallel_replicas = 0, query_plan_join_swap_table = 'auto', query_plan_optimize_join_order_limit = 10);
SELECT 'cross', 'dphyp', countIf(explain LIKE '%Type: cross%') FROM (EXPLAIN actions = 1 SELECT count() FROM a CROSS JOIN b JOIN f ON a.k = f.k AND b.k = f.k
    SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp', enable_join_transitive_predicates = 1, query_plan_optimize_join_order_randomize = 0, use_statistics = 0, use_hash_table_stats_for_join_reordering = 0, enable_parallel_replicas = 0, query_plan_join_swap_table = 'auto', query_plan_optimize_join_order_limit = 10);

SELECT 'range', (SELECT count() FROM f JOIN a ON f.k = a.k JOIN b ON f.k = b.k AND a.k < b.k);
SELECT 'range', 'dpsub', countIf(explain LIKE '%Type: cross%') FROM (EXPLAIN actions = 1 SELECT count() FROM f JOIN a ON f.k = a.k JOIN b ON f.k = b.k AND a.k < b.k
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsub', enable_join_transitive_predicates = 1, query_plan_optimize_join_order_randomize = 0, use_statistics = 0, use_hash_table_stats_for_join_reordering = 0, enable_parallel_replicas = 0, query_plan_join_swap_table = 'auto', query_plan_optimize_join_order_limit = 10);
SELECT 'range', 'greedy', countIf(explain LIKE '%Type: cross%') FROM (EXPLAIN actions = 1 SELECT count() FROM f JOIN a ON f.k = a.k JOIN b ON f.k = b.k AND a.k < b.k
    SETTINGS query_plan_optimize_join_order_algorithm = 'greedy', enable_join_transitive_predicates = 1, query_plan_optimize_join_order_randomize = 0, use_statistics = 0, use_hash_table_stats_for_join_reordering = 0, enable_parallel_replicas = 0, query_plan_join_swap_table = 'auto', query_plan_optimize_join_order_limit = 10);
SELECT 'range', 'dpsize', countIf(explain LIKE '%Type: cross%') FROM (EXPLAIN actions = 1 SELECT count() FROM f JOIN a ON f.k = a.k JOIN b ON f.k = b.k AND a.k < b.k
    SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize', enable_join_transitive_predicates = 1, query_plan_optimize_join_order_randomize = 0, use_statistics = 0, use_hash_table_stats_for_join_reordering = 0, enable_parallel_replicas = 0, query_plan_join_swap_table = 'auto', query_plan_optimize_join_order_limit = 10);
SELECT 'range', 'dphyp', countIf(explain LIKE '%Type: cross%') FROM (EXPLAIN actions = 1 SELECT count() FROM f JOIN a ON f.k = a.k JOIN b ON f.k = b.k AND a.k < b.k
    SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp', enable_join_transitive_predicates = 1, query_plan_optimize_join_order_randomize = 0, use_statistics = 0, use_hash_table_stats_for_join_reordering = 0, enable_parallel_replicas = 0, query_plan_join_swap_table = 'auto', query_plan_optimize_join_order_limit = 10);
