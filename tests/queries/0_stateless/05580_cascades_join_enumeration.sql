-- Tags: no-fasttest
-- With `cascades_join_enumeration`, Cascades gets one memo group per set of joined tables and a join for every split
-- of the set that DPsub accepted ('all_splits'), or only for the splits of the orders it would import ('candidates').
-- Every join of a set must give the same rows. Each query runs in every mode and on one node, and must return the same
-- result; the profile events show which graphs got their sets.
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET param__internal_cascades_cluster_node_count = 8;
SET distributed_plan_execute_locally = 1;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_algorithm = 'dpsub,greedy';
SET enable_join_runtime_filters = 0;
SET collect_hash_table_stats_during_joins = 0;
SET use_hash_table_stats_for_join_reordering = 0;
SET max_rows_to_group_by = 0;
SET join_algorithm = 'hash';

DROP TABLE IF EXISTS t_enum_a;
DROP TABLE IF EXISTS t_enum_b;
DROP TABLE IF EXISTS t_enum_c;
DROP TABLE IF EXISTS t_enum_d;
DROP TABLE IF EXISTS t_enum_e;
DROP TABLE IF EXISTS t_enum_f;

CREATE TABLE t_enum_a (id UInt32, k Nullable(UInt32), x UInt32, y UInt32, f Float64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_enum_b (id UInt32, k Nullable(UInt32), v UInt32, f Float64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_enum_c (id UInt32, k UInt32, w UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_enum_d (id UInt32, k UInt32, s String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_enum_e (id UInt32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_enum_f (id UInt32) ENGINE = MergeTree ORDER BY id;

-- Duplicate keys and NULL keys.
INSERT INTO t_enum_a SELECT number % 7, if(number % 5 = 0, NULL, number % 3), number % 4, number % 6, number / 2 FROM numbers(40);
INSERT INTO t_enum_b SELECT number % 9, if(number % 4 = 0, NULL, number % 3), number, number / 2 FROM numbers(30);
INSERT INTO t_enum_c SELECT number % 5, number % 3, number FROM numbers(25);
INSERT INTO t_enum_d SELECT number % 6, number % 4, toString(number % 2) FROM numbers(20);
INSERT INTO t_enum_e SELECT number FROM numbers(3);
-- t_enum_f stays empty.

-- Each query runs with a mode, then on one node: the results must be equal.
SELECT '-- chain';
SELECT 'off', count(), sum(a.x + b.v + c.w) FROM t_enum_a a JOIN t_enum_b b ON a.id = b.id JOIN t_enum_c c ON b.id = c.id JOIN t_enum_d d ON c.id = d.id
    SETTINGS cascades_join_enumeration = 'off', log_comment = 'join_enum_chain_off';
SELECT 'candidates', count(), sum(a.x + b.v + c.w) FROM t_enum_a a JOIN t_enum_b b ON a.id = b.id JOIN t_enum_c c ON b.id = c.id JOIN t_enum_d d ON c.id = d.id
    SETTINGS cascades_join_enumeration = 'candidates', log_comment = 'join_enum_chain_candidates';
SELECT 'all_splits', count(), sum(a.x + b.v + c.w) FROM t_enum_a a JOIN t_enum_b b ON a.id = b.id JOIN t_enum_c c ON b.id = c.id JOIN t_enum_d d ON c.id = d.id
    SETTINGS cascades_join_enumeration = 'all_splits', log_comment = 'join_enum_chain_all_splits';
SELECT 'all_splits_rule', count(), sum(a.x + b.v + c.w) FROM t_enum_a a JOIN t_enum_b b ON a.id = b.id JOIN t_enum_c c ON b.id = c.id JOIN t_enum_d d ON c.id = d.id
    SETTINGS cascades_join_enumeration = 'all_splits_rule', log_comment = 'join_enum_chain_rule';
SELECT 'topk', count(), sum(a.x + b.v + c.w) FROM t_enum_a a JOIN t_enum_b b ON a.id = b.id JOIN t_enum_c c ON b.id = c.id JOIN t_enum_d d ON c.id = d.id
    SETTINGS cascades_join_enumeration = 'topk', cascades_join_order_alternatives = 2, log_comment = 'join_enum_chain_topk';
SELECT 'winners', count(), sum(a.x + b.v + c.w) FROM t_enum_a a JOIN t_enum_b b ON a.id = b.id JOIN t_enum_c c ON b.id = c.id JOIN t_enum_d d ON c.id = d.id
    SETTINGS cascades_join_enumeration = 'winners', log_comment = 'join_enum_chain_winners';
SELECT 'winners_candidates', count(), sum(a.x + b.v + c.w) FROM t_enum_a a JOIN t_enum_b b ON a.id = b.id JOIN t_enum_c c ON b.id = c.id JOIN t_enum_d d ON c.id = d.id
    SETTINGS cascades_join_enumeration = 'winners_candidates', log_comment = 'join_enum_chain_winners_candidates';
SELECT 'best_splits', count(), sum(a.x + b.v + c.w) FROM t_enum_a a JOIN t_enum_b b ON a.id = b.id JOIN t_enum_c c ON b.id = c.id JOIN t_enum_d d ON c.id = d.id
    SETTINGS cascades_join_enumeration = 'best_splits', cascades_join_enumeration_splits_per_set = 2, log_comment = 'join_enum_chain_best_splits';
-- The joins that DPhyp accepted in place of those of DPsub.
SELECT 'all_splits dphyp', count(), sum(a.x + b.v + c.w) FROM t_enum_a a JOIN t_enum_b b ON a.id = b.id JOIN t_enum_c c ON b.id = c.id JOIN t_enum_d d ON c.id = d.id
    SETTINGS cascades_join_enumeration = 'all_splits', query_plan_optimize_join_order_algorithm = 'dphyp,greedy', log_comment = 'join_enum_chain_dphyp';
SELECT 'best_splits dphyp', count(), sum(a.x + b.v + c.w) FROM t_enum_a a JOIN t_enum_b b ON a.id = b.id JOIN t_enum_c c ON b.id = c.id JOIN t_enum_d d ON c.id = d.id
    SETTINGS cascades_join_enumeration = 'best_splits', query_plan_optimize_join_order_algorithm = 'dphyp,greedy', log_comment = 'join_enum_chain_best_splits_dphyp';
-- A budget that one join per set does not fit: the graph gets the joins of the orders.
SELECT 'best_splits budget', count(), sum(a.x + b.v + c.w) FROM t_enum_a a JOIN t_enum_b b ON a.id = b.id JOIN t_enum_c c ON b.id = c.id JOIN t_enum_d d ON c.id = d.id
    SETTINGS cascades_join_enumeration = 'best_splits', cascades_join_enumeration_max_set_joins = 1, log_comment = 'join_enum_chain_best_splits_budget';
SELECT 'one node', count(), sum(a.x + b.v + c.w) FROM t_enum_a a JOIN t_enum_b b ON a.id = b.id JOIN t_enum_c c ON b.id = c.id JOIN t_enum_d d ON c.id = d.id
    SETTINGS make_distributed_plan = 0;

SELECT '-- transitive chain, transitive predicates on and off';
SELECT 'all_splits on', count(), sum(a.x * d.k) FROM t_enum_a a JOIN t_enum_b b ON a.id = b.id JOIN t_enum_c c ON b.id = c.id JOIN t_enum_d d ON c.id = d.id
    SETTINGS cascades_join_enumeration = 'all_splits', enable_join_transitive_predicates = 1, log_comment = 'join_enum_transitive_on';
SELECT 'all_splits off', count(), sum(a.x * d.k) FROM t_enum_a a JOIN t_enum_b b ON a.id = b.id JOIN t_enum_c c ON b.id = c.id JOIN t_enum_d d ON c.id = d.id
    SETTINGS cascades_join_enumeration = 'all_splits', enable_join_transitive_predicates = 0, log_comment = 'join_enum_transitive_off';
SELECT 'one node', count(), sum(a.x * d.k) FROM t_enum_a a JOIN t_enum_b b ON a.id = b.id JOIN t_enum_c c ON b.id = c.id JOIN t_enum_d d ON c.id = d.id
    SETTINGS make_distributed_plan = 0;

SELECT '-- cycle with several keys, nullable keys';
SELECT 'all_splits', count(), sum(c.w) FROM t_enum_a a JOIN t_enum_b b ON a.id = b.id AND a.k = b.k JOIN t_enum_c c ON b.k = c.k JOIN t_enum_d d ON c.id = d.id AND d.k = a.x
    SETTINGS cascades_join_enumeration = 'all_splits', enable_join_transitive_predicates = 1, log_comment = 'join_enum_cycle';
SELECT 'all_splits_rule', count(), sum(c.w) FROM t_enum_a a JOIN t_enum_b b ON a.id = b.id AND a.k = b.k JOIN t_enum_c c ON b.k = c.k JOIN t_enum_d d ON c.id = d.id AND d.k = a.x
    SETTINGS cascades_join_enumeration = 'all_splits_rule', enable_join_transitive_predicates = 1, log_comment = 'join_enum_cycle_rule';
SELECT 'one node', count(), sum(c.w) FROM t_enum_a a JOIN t_enum_b b ON a.id = b.id AND a.k = b.k JOIN t_enum_c c ON b.k = c.k JOIN t_enum_d d ON c.id = d.id AND d.k = a.x
    SETTINGS make_distributed_plan = 0;

SELECT '-- null-safe keys';
SELECT 'all_splits', count() FROM t_enum_a a JOIN t_enum_b b ON a.k <=> b.k JOIN t_enum_c c ON b.id = c.id
    SETTINGS cascades_join_enumeration = 'all_splits', log_comment = 'join_enum_null_safe';
SELECT 'one node', count() FROM t_enum_a a JOIN t_enum_b b ON a.k <=> b.k JOIN t_enum_c c ON b.id = c.id
    SETTINGS make_distributed_plan = 0;

SELECT '-- a predicate over three tables, a one-sided condition, a self-equality, a constant, computed outputs';
SELECT 'all_splits', count(), sum(a.x + c.w), max(d.s || toString(a.y))
FROM t_enum_a a JOIN t_enum_b b ON a.id = b.id JOIN t_enum_c c ON b.id = c.id AND a.x + b.v > c.w AND c.k = c.k AND 1 = 1 JOIN t_enum_d d ON c.id = d.id AND d.s = '1'
    SETTINGS cascades_join_enumeration = 'all_splits', log_comment = 'join_enum_predicates';
SELECT 'all_splits_rule', count(), sum(a.x + c.w), max(d.s || toString(a.y))
FROM t_enum_a a JOIN t_enum_b b ON a.id = b.id JOIN t_enum_c c ON b.id = c.id AND a.x + b.v > c.w AND c.k = c.k AND 1 = 1 JOIN t_enum_d d ON c.id = d.id AND d.s = '1'
    SETTINGS cascades_join_enumeration = 'all_splits_rule', log_comment = 'join_enum_predicates_rule';
SELECT 'one node', count(), sum(a.x + c.w), max(d.s || toString(a.y))
FROM t_enum_a a JOIN t_enum_b b ON a.id = b.id JOIN t_enum_c c ON b.id = c.id AND a.x + b.v > c.w AND c.k = c.k AND 1 = 1 JOIN t_enum_d d ON c.id = d.id AND d.s = '1'
    SETTINGS make_distributed_plan = 0;

SELECT '-- count over a cross product, and a cross product with an empty table';
SELECT 'all_splits', count() FROM t_enum_a a JOIN t_enum_b b ON a.id = b.id JOIN t_enum_c c ON b.v = c.w CROSS JOIN t_enum_e e
    SETTINGS cascades_join_enumeration = 'all_splits', log_comment = 'join_enum_cross';
SELECT 'one node', count() FROM t_enum_a a JOIN t_enum_b b ON a.id = b.id JOIN t_enum_c c ON b.v = c.w CROSS JOIN t_enum_e e
    SETTINGS make_distributed_plan = 0;
SELECT 'all_splits', count() FROM t_enum_a a JOIN t_enum_b b ON a.id = b.id JOIN t_enum_c c ON b.id = c.id CROSS JOIN t_enum_f f
    SETTINGS cascades_join_enumeration = 'all_splits', log_comment = 'join_enum_cross_empty';

SELECT '-- graphs that keep their join orders';
-- Two columns of one table in one equivalence class.
SELECT 'two members', count() FROM t_enum_a a JOIN t_enum_b b ON a.x = b.id JOIN t_enum_c c ON a.y = c.id JOIN t_enum_d d ON b.id = d.id AND c.id = d.id
    SETTINGS cascades_join_enumeration = 'all_splits', enable_join_transitive_predicates = 1, log_comment = 'join_enum_two_members';
SELECT 'one node', count() FROM t_enum_a a JOIN t_enum_b b ON a.x = b.id JOIN t_enum_c c ON a.y = c.id JOIN t_enum_d d ON b.id = d.id AND c.id = d.id
    SETTINGS make_distributed_plan = 0;
-- A floating-point key.
SELECT 'float key', count() FROM t_enum_a a JOIN t_enum_b b ON a.f = b.f JOIN t_enum_c c ON b.id = c.id
    SETTINGS cascades_join_enumeration = 'all_splits', log_comment = 'join_enum_float';
-- An equality whose operand reads two tables.
SELECT 'two-table operand', count() FROM t_enum_a a JOIN t_enum_b b ON a.id = b.id JOIN t_enum_c c ON b.id = c.id AND a.x + b.v = c.w
    SETTINGS cascades_join_enumeration = 'all_splits', log_comment = 'join_enum_two_table_operand';
SELECT 'one node', count() FROM t_enum_a a JOIN t_enum_b b ON a.id = b.id JOIN t_enum_c c ON b.id = c.id AND a.x + b.v = c.w
    SETTINGS make_distributed_plan = 0;
-- `arrayJoin` in a condition: its join stays out of the reordered tables, which still get their sets.
SELECT 'arrayJoin', count() FROM t_enum_a a JOIN t_enum_b b ON a.id = b.id JOIN t_enum_c c ON b.id = c.id JOIN t_enum_d d ON c.id = d.id AND arrayJoin([1, 1]) > 0
    SETTINGS cascades_join_enumeration = 'all_splits', query_plan_optimize_join_order_conflict_detector = '', log_comment = 'join_enum_array_join';
SELECT 'one node', count() FROM t_enum_a a JOIN t_enum_b b ON a.id = b.id JOIN t_enum_c c ON b.id = c.id JOIN t_enum_d d ON c.id = d.id AND arrayJoin([1, 1]) > 0
    SETTINGS make_distributed_plan = 0;
-- Too many splits for the limit.
SELECT 'limit', count() FROM t_enum_a a JOIN t_enum_b b ON a.id = b.id JOIN t_enum_c c ON b.id = c.id JOIN t_enum_d d ON c.id = d.id
    SETTINGS cascades_join_enumeration = 'all_splits', cascades_join_enumeration_max_splits = 2, log_comment = 'join_enum_limit';

SYSTEM FLUSH LOGS query_log;
-- `query_log` merges its parts while the test reads it; a distributed read is not what the test checks.
SELECT '-- sets given to Cascades';
SELECT log_comment, ProfileEvents['JoinEnumerationSetPlans'] AS graphs, ProfileEvents['JoinEnumerationSplits'] > 0 AS has_splits,
    ProfileEvents['JoinEnumerationRuleExpansions'] AS rule_expansions
FROM system.query_log
WHERE current_database = currentDatabase() AND is_initial_query AND type = 'QueryFinish' AND log_comment LIKE 'join\_enum\_%' AND event_date >= yesterday()
ORDER BY log_comment
SETTINGS make_distributed_plan = 0;
SELECT '-- the rule adds the joins that the import of all splits gets';
SELECT replaceOne(replaceOne(log_comment, '_rule', ''), '_all_splits', '') AS graph, count() AS runs, uniqExact(ProfileEvents['JoinEnumerationSplits']) AS split_counts
FROM system.query_log
WHERE current_database = currentDatabase() AND is_initial_query AND type = 'QueryFinish' AND event_date >= yesterday()
    AND log_comment IN ('join_enum_chain_all_splits', 'join_enum_chain_rule', 'join_enum_cycle', 'join_enum_cycle_rule', 'join_enum_predicates', 'join_enum_predicates_rule')
GROUP BY 1 ORDER BY 1
SETTINGS make_distributed_plan = 0;

DROP TABLE t_enum_a;
DROP TABLE t_enum_b;
DROP TABLE t_enum_c;
DROP TABLE t_enum_d;
DROP TABLE t_enum_e;
DROP TABLE t_enum_f;
