-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-settings, no-random-merge-tree-settings: EXPLAIN output may differ

-- With a row policy as the only filter, the projections of `SELECT * FROM (SELECT * FROM t)` merge into one
-- ExpressionStep below the sort that renames `id -> __table2.id -> id -> __table1.id`. The middle `id` reuses
-- the input's name, but it is only read through an alias, so splitting the DAG for lazy materialization does
-- not rename it, and the optimization must still apply (it did not since 26.8, issue #127157).

DROP TABLE IF EXISTS t_05441;
CREATE TABLE t_05441 (id UInt64, tenant String, payload String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_05441 SELECT number, if(number % 2 = 0, 'a', 'b'), toString(number) FROM numbers(1000);

CREATE ROW POLICY rp_05441 ON t_05441 FOR SELECT USING tenant = 'a' TO CURRENT_USER;

SET query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10;
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SELECT 'table';
SELECT count() FROM (EXPLAIN SELECT * FROM t_05441 ORDER BY id LIMIT 3 OFFSET 100) WHERE explain LIKE '%LazilyReadFromMergeTree%';

SELECT 'subquery';
SELECT count() FROM (EXPLAIN SELECT * FROM (SELECT * FROM t_05441) ORDER BY id LIMIT 3 OFFSET 100) WHERE explain LIKE '%LazilyReadFromMergeTree%';
SELECT * FROM (SELECT * FROM t_05441) ORDER BY id LIMIT 3 OFFSET 100;
SELECT * FROM (SELECT * FROM t_05441) ORDER BY id LIMIT 3 OFFSET 100 SETTINGS query_plan_optimize_lazy_materialization = 0;

SELECT 'nested subquery';
SELECT count() FROM (EXPLAIN SELECT * FROM (SELECT * FROM (SELECT * FROM t_05441)) ORDER BY id LIMIT 3 OFFSET 100) WHERE explain LIKE '%LazilyReadFromMergeTree%';
SELECT * FROM (SELECT * FROM (SELECT * FROM t_05441)) ORDER BY id LIMIT 3 OFFSET 100;

SELECT 'subquery, sort by payload';
SELECT * FROM (SELECT * FROM t_05441) ORDER BY payload, id LIMIT 3 OFFSET 100;
SELECT * FROM (SELECT * FROM t_05441) ORDER BY payload, id LIMIT 3 OFFSET 100 SETTINGS query_plan_optimize_lazy_materialization = 0;

DROP ROW POLICY rp_05441 ON t_05441;
DROP TABLE t_05441;
