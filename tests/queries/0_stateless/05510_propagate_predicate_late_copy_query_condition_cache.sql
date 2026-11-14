-- a copy onto a column outside the primary key is added after index analysis: the query condition
-- cache entry of the target's own filter must not record the granules that only the copy emptied
SET enable_parallel_replicas = 0, enable_join_runtime_filters = 0, use_query_condition_cache = 1;
SET optimize_move_to_prewhere = 0, query_plan_propagate_predicate_across_join = 1;
SET query_plan_join_swap_table = 'false', query_plan_optimize_join_order_limit = 0;

DROP TABLE IF EXISTS late_copy_qcc_s;
DROP TABLE IF EXISTS late_copy_qcc_d;

CREATE TABLE late_copy_qcc_s (k UInt64) ENGINE = Memory;
CREATE TABLE late_copy_qcc_d (k UInt64, pad UInt64) ENGINE = MergeTree ORDER BY pad SETTINGS index_granularity = 8192;
INSERT INTO late_copy_qcc_s VALUES (42);
INSERT INTO late_copy_qcc_d SELECT number, number FROM numbers(100000);

SELECT 'copied', count() FROM (
    EXPLAIN SELECT count() FROM (SELECT k FROM late_copy_qcc_s WHERE k = 42) AS s
    INNER JOIN (SELECT k FROM late_copy_qcc_d WHERE pad % 2 = 0) AS d ON s.k = d.k)
WHERE explain LIKE '%Propagated equi-join filter%';

SELECT 'join', count() FROM (SELECT k FROM late_copy_qcc_s WHERE k = 42) AS s
INNER JOIN (SELECT k FROM late_copy_qcc_d WHERE pad % 2 = 0) AS d ON s.k = d.k;

SELECT 'target filter alone', count() FROM late_copy_qcc_d WHERE pad % 2 = 0;

DROP TABLE late_copy_qcc_s;
DROP TABLE late_copy_qcc_d;
