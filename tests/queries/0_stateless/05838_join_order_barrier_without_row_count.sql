-- Join reordering takes a relation only if it has a row count. A relation without one is a barrier: the joins above it keep
-- their written order and sides, and the joins below it are still reordered.
-- https://github.com/ClickHouse/ClickHouse/issues/129254

SET query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0, query_plan_join_swap_table = 'auto',
    use_hash_table_stats_for_join_reordering = 0, enable_join_runtime_filters = 0, enable_join_transitive_predicates = 0,
    enable_parallel_replicas = 0, explain_query_plan_default = 'legacy';

CREATE TABLE orders (o_orderkey Int32, o_custkey Int32) ENGINE = MergeTree ORDER BY o_orderkey SETTINGS auto_statistics_types = '';
CREATE TABLE customer (c_custkey Int32, c_nationkey Int32) ENGINE = MergeTree ORDER BY c_custkey SETTINGS auto_statistics_types = '';
CREATE TABLE nation (n_nationkey Int32, n_name String) ENGINE = MergeTree ORDER BY n_nationkey SETTINGS auto_statistics_types = '';

INSERT INTO orders SELECT number, number % 20000 FROM numbers(100000);
INSERT INTO customer SELECT number, number % 3 + 5 FROM numbers(10000);
INSERT INTO nation VALUES (5, 'ETHIOPIA'), (6, 'FRANCE'), (7, 'GERMANY'), (100, 'UNKNOWN');

SELECT 'a filter that neither the index nor statistics estimate keeps an assumed share of the rows';
SELECT trimLeft(explain) FROM (EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT count() FROM orders
    JOIN (SELECT * FROM customer JOIN nation ON c_nationkey = n_nationkey WHERE n_name = 'GERMANY') AS cn ON c_custkey = o_custkey)
WHERE explain LIKE '%Join: %';

SELECT 'two relations: an assumed row count does not swap the written sides';
SELECT trimLeft(explain) FROM (EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT count() FROM (SELECT * FROM nation WHERE n_name = 'GERMANY') AS n JOIN orders ON n_nationkey = o_custkey)
WHERE explain LIKE '%Join: %';

SELECT 'the assumed share is a setting';
SELECT trimLeft(explain) FROM (EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT count() FROM orders JOIN customer ON c_custkey = o_custkey JOIN nation ON c_nationkey = n_nationkey WHERE o_custkey < 15000
    SETTINGS query_plan_join_order_default_filter_selectivity = 0.5)
WHERE explain LIKE '%Join: %';

SELECT 'with 0 nothing is assumed: the filtered tables have no row count and the joins are as written';
SELECT trimLeft(explain) FROM (EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT count() FROM orders JOIN customer ON c_custkey = o_custkey JOIN nation ON c_nationkey = n_nationkey WHERE o_custkey < 15000
    SETTINGS query_plan_join_order_default_filter_selectivity = 0)
WHERE explain LIKE '%Join: %';

EXPLAIN SELECT count() FROM orders JOIN customer ON c_custkey = o_custkey WHERE o_custkey < 15000
    SETTINGS query_plan_join_order_default_filter_selectivity = 1.5; -- { serverError INVALID_SETTING_VALUE }

-- A filtered `arrayJoin` has no row count.
SELECT 'a relation without a row count: the joins below it are reordered, the joins above it are as written';
SELECT trimLeft(explain) FROM (EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT count() FROM nation JOIN customer ON c_nationkey = n_nationkey JOIN orders ON o_custkey = c_custkey
    JOIN (SELECT arrayJoin([5, 6, 7]) AS k WHERE k != 6) AS u ON u.k = n_nationkey
    JOIN nation AS n2 ON n2.n_nationkey = c_nationkey)
WHERE explain LIKE '%Join: %';

SELECT 'a relation without a row count comes first: every join is as written';
SELECT trimLeft(explain) FROM (EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT count() FROM (SELECT arrayJoin([5, 6, 7]) AS k WHERE k != 6) AS u
    JOIN nation ON u.k = n_nationkey JOIN customer ON c_nationkey = n_nationkey JOIN orders ON o_custkey = c_custkey)
WHERE explain LIKE '%Join: %';

SELECT 'the result does not depend on the join order';
SELECT count() FROM nation JOIN customer ON c_nationkey = n_nationkey JOIN orders ON o_custkey = c_custkey
    JOIN (SELECT arrayJoin([5, 6, 7]) AS k WHERE k != 6) AS u ON u.k = n_nationkey
    JOIN nation AS n2 ON n2.n_nationkey = c_nationkey;
SELECT count() FROM nation JOIN customer ON c_nationkey = n_nationkey JOIN orders ON o_custkey = c_custkey
    JOIN (SELECT arrayJoin([5, 6, 7]) AS k WHERE k != 6) AS u ON u.k = n_nationkey
    JOIN nation AS n2 ON n2.n_nationkey = c_nationkey
SETTINGS query_plan_optimize_join_order_limit = 0;
