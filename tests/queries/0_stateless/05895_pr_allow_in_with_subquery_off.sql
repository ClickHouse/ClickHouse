-- A subquery whose own SETTINGS set parallel_replicas_allow_in_with_subquery = 0 and that filters by IN (subquery)
-- is read once, not once per replica. index_granularity = 1 gives every replica marks.

DROP VIEW IF EXISTS v_pr_in;
DROP TABLE IF EXISTS t_pr_in;
DROP TABLE IF EXISTS t_pr_in_set;
DROP TABLE IF EXISTS t_pr_in_r;

CREATE TABLE t_pr_in (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_pr_in_set (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_pr_in_r (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_pr_in SELECT number FROM numbers(300);
INSERT INTO t_pr_in_set SELECT number FROM numbers(10);
INSERT INTO t_pr_in_r SELECT number FROM numbers(300);

SET automatic_parallel_replicas_mode = 0, parallel_replicas_plan_based = 0, parallel_replicas_min_number_of_rows_per_replica = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_for_queries_with_multiple_tables = 1;

SELECT '-- results';
SELECT count(), sum(a) FROM (SELECT a FROM t_pr_in WHERE a IN (SELECT number FROM numbers(10)));
SELECT count(), sum(a) FROM (SELECT a FROM t_pr_in WHERE a IN (SELECT number FROM numbers(10)) SETTINGS parallel_replicas_allow_in_with_subquery = 0) SETTINGS parallel_replicas_local_plan = 0;
SELECT count(), sum(a) FROM (SELECT a FROM t_pr_in WHERE a IN (SELECT number FROM numbers(10)) SETTINGS parallel_replicas_allow_in_with_subquery = 0) SETTINGS parallel_replicas_local_plan = 1;
SELECT count(), sum(a) FROM (SELECT a FROM (SELECT a FROM t_pr_in WHERE a IN (SELECT number FROM numbers(10))) SETTINGS parallel_replicas_allow_in_with_subquery = 0) SETTINGS parallel_replicas_local_plan = 0;
SELECT count(), sum(a) FROM (SELECT a FROM t_pr_in WHERE a NOT IN (SELECT number FROM numbers(290)) SETTINGS parallel_replicas_allow_in_with_subquery = 0) SETTINGS parallel_replicas_local_plan = 0;
SELECT count(), sum(a) FROM (SELECT a FROM t_pr_in WHERE a IN t_pr_in_set SETTINGS parallel_replicas_allow_in_with_subquery = 0) SETTINGS parallel_replicas_local_plan = 0;
SELECT count(), sum(l.a) FROM (SELECT a FROM t_pr_in WHERE a IN (SELECT number FROM numbers(10)) SETTINGS parallel_replicas_allow_in_with_subquery = 0) AS l INNER JOIN t_pr_in_r AS r USING a SETTINGS parallel_replicas_local_plan = 0;

SET parallel_replicas_filter_pushdown = 1;
SELECT '-- an IN above a subquery read';
SELECT count(), sum(a) FROM (SELECT a FROM t_pr_in) WHERE a IN (SELECT number FROM numbers(10)) SETTINGS parallel_replicas_allow_in_with_subquery = 0, parallel_replicas_local_plan = 0;
SELECT count(), sum(a) FROM (SELECT a FROM t_pr_in) WHERE a GLOBAL IN (SELECT number FROM numbers(10)) SETTINGS parallel_replicas_allow_in_with_subquery = 0, parallel_replicas_local_plan = 0;
SELECT count(), sum(a) FROM (SELECT a FROM t_pr_in) WHERE a IN (SELECT number FROM numbers(10)) AND a > 2 SETTINGS parallel_replicas_allow_in_with_subquery = 0, parallel_replicas_local_plan = 0;
SELECT count(), sum(a) FROM (SELECT a FROM t_pr_in) WHERE a IN (SELECT number FROM numbers(10)) SETTINGS parallel_replicas_allow_in_with_subquery = 0, parallel_replicas_local_plan = 1;
SELECT count(), sum(a) FROM (SELECT a FROM (SELECT a FROM t_pr_in) WHERE a IN (SELECT number FROM numbers(10)) SETTINGS parallel_replicas_allow_in_with_subquery = 0) SETTINGS parallel_replicas_local_plan = 0;
SELECT a FROM (SELECT a FROM t_pr_in) WHERE a IN (SELECT number FROM numbers(10)) AND a > 2 ORDER BY a LIMIT 3 SETTINGS parallel_replicas_allow_in_with_subquery = 0, parallel_replicas_local_plan = 1, optimize_read_in_order = 1;

CREATE VIEW v_pr_in AS SELECT a FROM t_pr_in WHERE a IN (SELECT number FROM numbers(10)) SETTINGS parallel_replicas_allow_in_with_subquery = 0;
SELECT count(), sum(a) FROM v_pr_in SETTINGS parallel_replicas_local_plan = 0;

SELECT '-- shipped to replicas';
SELECT 'plain', count() > 0 FROM (EXPLAIN SELECT sum(a) FROM (SELECT a FROM t_pr_in WHERE a IN (SELECT number FROM numbers(10)))) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%';
SELECT 'opt_out', count() > 0 FROM (EXPLAIN SELECT sum(a) FROM (SELECT a FROM t_pr_in WHERE a IN (SELECT number FROM numbers(10)) SETTINGS parallel_replicas_allow_in_with_subquery = 0)) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%';
SELECT 'opt_out_outer', count() > 0 FROM (EXPLAIN SELECT sum(a) FROM (SELECT a FROM (SELECT a FROM t_pr_in WHERE a IN (SELECT number FROM numbers(10))) SETTINGS parallel_replicas_allow_in_with_subquery = 0)) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%';
SELECT 'opt_out_root', count() > 0 FROM (EXPLAIN SELECT sum(a) FROM (SELECT a FROM t_pr_in WHERE a IN (SELECT number FROM numbers(10))) SETTINGS parallel_replicas_allow_in_with_subquery = 0) WHERE explain ILIKE '%ReadFromRemoteParallelReplicas%';
SELECT '-- what the replicas are sent: the range, never the IN';
SELECT count(), sum(a) FROM (SELECT a FROM t_pr_in) WHERE a IN (SELECT number FROM numbers(10)) AND a > 2
SETTINGS parallel_replicas_allow_in_with_subquery = 0, parallel_replicas_local_plan = 0, parallel_replicas_filter_pushdown = 1,
    serialize_query_plan = 0, allow_push_predicate_ast_for_distributed_subqueries = 1, log_comment = '05895_in_and_range'
FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT
    count() > 0 AS sent_a_query,
    countIf(query ILIKE '%HAVING%greater(%') = count() AS all_carry_the_range,
    countIf(query ILIKE '%HAVING%in(%') = 0 AS none_carry_the_in
FROM system.query_log
WHERE has(databases, currentDatabase()) AND log_comment = '05895_in_and_range' AND NOT is_initial_query
SETTINGS enable_parallel_replicas = 0;

DROP VIEW v_pr_in;
DROP TABLE t_pr_in;
DROP TABLE t_pr_in_set;
DROP TABLE t_pr_in_r;
