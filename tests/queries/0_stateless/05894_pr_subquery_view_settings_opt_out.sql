-- A subquery or a view body whose own SETTINGS turn parallel replicas off is read once, not once per replica.
-- index_granularity = 1 gives every replica marks; the EXPLAIN section checks that nothing is shipped to the replicas.

DROP VIEW IF EXISTS v_off_nested;
DROP VIEW IF EXISTS v_off_over_leaf;
DROP VIEW IF EXISTS v_off_sub;
DROP VIEW IF EXISTS v_on;
DROP VIEW IF EXISTS v_off;
DROP VIEW IF EXISTS v_mpr1;
DROP VIEW IF EXISTS v_ck;
DROP VIEW IF EXISTS v_leaf;
DROP TABLE IF EXISTS t_pr;
DROP TABLE IF EXISTS t_pr_r;

CREATE TABLE t_pr (a Int32) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_pr_r (a Int32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_pr SELECT number FROM numbers(300);
INSERT INTO t_pr_r SELECT number FROM numbers(300);

CREATE VIEW v_leaf AS SELECT a FROM t_pr;
CREATE VIEW v_on AS SELECT a FROM t_pr SETTINGS enable_parallel_replicas = 1;
CREATE VIEW v_off AS SELECT a FROM t_pr SETTINGS enable_parallel_replicas = 0;
CREATE VIEW v_mpr1 AS SELECT a FROM t_pr SETTINGS max_parallel_replicas = 1;
CREATE VIEW v_ck AS SELECT a FROM t_pr SETTINGS parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'a';
CREATE VIEW v_off_sub AS SELECT a FROM (SELECT a FROM t_pr SETTINGS enable_parallel_replicas = 0);
CREATE VIEW v_off_nested AS SELECT a FROM v_off;
CREATE VIEW v_off_over_leaf AS SELECT a FROM v_leaf SETTINGS enable_parallel_replicas = 0;

SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_allow_view_over_mergetree = 1, parallel_replicas_for_queries_with_multiple_tables = 1;

SELECT '-- subqueries';
SELECT count(), sum(a) FROM (SELECT a FROM t_pr);
SELECT count(), sum(a) FROM (SELECT a FROM t_pr SETTINGS enable_parallel_replicas = 0);
SELECT count(), sum(a) FROM (SELECT a FROM t_pr SETTINGS max_parallel_replicas = 1);
SELECT count(), sum(a) FROM (SELECT a FROM t_pr SETTINGS parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'a');
SELECT count(), sum(a) FROM (SELECT l.a AS a FROM t_pr AS l INNER JOIN t_pr_r AS r USING a SETTINGS parallel_replicas_for_queries_with_multiple_tables = 0);
SELECT count(), sum(a) FROM (SELECT l.a AS a FROM t_pr AS l INNER JOIN t_pr_r AS r USING a SETTINGS parallel_replicas_for_queries_with_multiple_tables = 1) SETTINGS parallel_replicas_for_queries_with_multiple_tables = 0;

SELECT '-- views';
SELECT count(), sum(a) FROM v_on;
SELECT count(), sum(a) FROM v_off;
SELECT count(), sum(a) FROM v_mpr1;
SELECT count(), sum(a) FROM v_ck;
SELECT count(), sum(a) FROM v_off_sub;
SELECT count(), sum(a) FROM v_off_nested;
SELECT count(), sum(a) FROM v_off_over_leaf;

-- Shipped SQL only exists with query-based parallel replicas.
SET parallel_replicas_plan_based = 0;

SELECT '-- shipped to replicas';
SELECT 'sub_plain', count() > 0 FROM (EXPLAIN SELECT sum(a) FROM (SELECT a FROM t_pr)) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%';
SELECT 'sub_off', count() > 0 FROM (EXPLAIN SELECT sum(a) FROM (SELECT a FROM t_pr SETTINGS enable_parallel_replicas = 0)) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%';
SELECT 'sub_mpr1', count() > 0 FROM (EXPLAIN SELECT sum(a) FROM (SELECT a FROM t_pr SETTINGS max_parallel_replicas = 1)) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%';
SELECT 'sub_join_off', count() > 0 FROM (EXPLAIN SELECT sum(a) FROM (SELECT l.a AS a FROM t_pr AS l INNER JOIN t_pr_r AS r USING a SETTINGS parallel_replicas_for_queries_with_multiple_tables = 0)) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%';
SELECT 'v_on', count() > 0 FROM (EXPLAIN SELECT sum(a) FROM v_on) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%';
SELECT 'v_off', count() > 0 FROM (EXPLAIN SELECT sum(a) FROM v_off) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%';
SELECT 'v_mpr1', count() > 0 FROM (EXPLAIN SELECT sum(a) FROM v_mpr1) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%';
SELECT 'v_ck', count() > 0 FROM (EXPLAIN SELECT sum(a) FROM v_ck) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%';
SELECT 'v_off_sub', count() > 0 FROM (EXPLAIN SELECT sum(a) FROM v_off_sub) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%';
SELECT 'v_off_nested', count() > 0 FROM (EXPLAIN SELECT sum(a) FROM v_off_nested) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%';
SELECT 'v_off_over_leaf', count() > 0 FROM (EXPLAIN SELECT sum(a) FROM v_off_over_leaf) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%';

DROP VIEW v_off_nested;
DROP VIEW v_off_over_leaf;
DROP VIEW v_off_sub;
DROP VIEW v_on;
DROP VIEW v_off;
DROP VIEW v_mpr1;
DROP VIEW v_ck;
DROP VIEW v_leaf;
DROP TABLE t_pr;
DROP TABLE t_pr_r;
