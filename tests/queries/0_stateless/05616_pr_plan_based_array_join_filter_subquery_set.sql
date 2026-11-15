-- An `IN` filter on an `ARRAY JOIN`ed column is fused into the `ARRAY JOIN` step. With plan-based parallel replicas,
-- a fragment whose `IN` set cannot be rebuilt on a replica (a non-deterministic or non-serializable subquery) must
-- stay local, also when the set is in the fused filter of the `ARRAY JOIN` step.

-- Fusion is skipped for serialized plans; pin it so the fused filter is present in every suite.
SET serialize_query_plan = 0;

DROP TABLE IF EXISTS t_pr_aj_set_left;
DROP TABLE IF EXISTS t_pr_aj_set_right;

CREATE TABLE t_pr_aj_set_left (id UInt64) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 128;
INSERT INTO t_pr_aj_set_left SELECT number % 10 FROM numbers(100000);

CREATE TABLE t_pr_aj_set_right (id UInt64, arr Array(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_pr_aj_set_right SELECT number, ['', 'b', 'c'] FROM numbers(10);

SET enable_parallel_replicas = 1;
SET parallel_replicas_plan_based = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET automatic_parallel_replicas_mode = 0;
-- Without the local plan every replica, the local one included, runs a shipped fragment and rebuilds its sets.
SET parallel_replicas_local_plan = 0;
-- The `ARRAY JOIN` side has to stay the broadcast one, which is shipped with the fragment.
SET query_plan_join_swap_table = false;

SELECT '--- a non-deterministic set has one value, so exactly one element matches';
-- A wrong result needs two replicas reading the left table, so it is rare; the plan shows the fragment stays local.
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') = 0 FROM (EXPLAIN SELECT uniqExact(r.elem), count() FROM t_pr_aj_set_left AS l
INNER JOIN (SELECT id, elem FROM t_pr_aj_set_right ARRAY JOIN arr AS elem) AS r ON l.id = r.id
WHERE r.elem IN (SELECT if(rand() % 2 = 0, 'b', 'c') FROM numbers(1) SETTINGS enable_parallel_replicas = 0));
SELECT uniqExact(r.elem), count() FROM t_pr_aj_set_left AS l
INNER JOIN (SELECT id, elem FROM t_pr_aj_set_right ARRAY JOIN arr AS elem) AS r ON l.id = r.id
WHERE r.elem IN (SELECT if(rand() % 2 = 0, 'b', 'c') FROM numbers(1) SETTINGS enable_parallel_replicas = 0);

SELECT '--- a set from a non-serializable source';
SELECT count() > 0 FROM (EXPLAIN SELECT count() FROM t_pr_aj_set_left AS l
INNER JOIN (SELECT id, elem FROM t_pr_aj_set_right ARRAY JOIN arr AS elem) AS r ON l.id = r.id
WHERE r.elem IN (SELECT x FROM generateRandom('x String', 1, 0, 1) LIMIT 3));
SELECT count() FROM t_pr_aj_set_left AS l
INNER JOIN (SELECT id, elem FROM t_pr_aj_set_right ARRAY JOIN arr AS elem) AS r ON l.id = r.id
WHERE r.elem IN (SELECT x FROM generateRandom('x String', 1, 0, 1) LIMIT 3);

DROP TABLE t_pr_aj_set_left;
DROP TABLE t_pr_aj_set_right;
