-- Automatic parallel replicas remembers a plan with parallel replicas it could not use, so that repeating the query
-- does not build it again: neither outcome records statistics, and without this every execution paid for the build.

DROP TABLE IF EXISTS t_remember_unmatched_l;
DROP TABLE IF EXISTS t_remember_unmatched_o;

CREATE TABLE t_remember_unmatched_l (l_orderkey UInt64, l_price Float64, l_discount Float64) ENGINE = MergeTree ORDER BY l_orderkey;
CREATE TABLE t_remember_unmatched_o (o_orderkey UInt64, o_total Float64) ENGINE = MergeTree ORDER BY o_orderkey;
INSERT INTO t_remember_unmatched_l SELECT number % 10000, number % 1000, (number % 10) / 100 FROM numbers(100000);
INSERT INTO t_remember_unmatched_o SELECT number, number % 1000 FROM numbers(10000);

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1, automatic_parallel_replicas_remember_boundary = 1,
    parallel_replicas_local_plan = 1, parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_for_queries_with_multiple_tables = 1,
    max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    automatic_parallel_replicas_min_bytes_per_replica = 0, serialize_query_plan = 0;

-- A correlated subquery: the planner does not use parallel replicas in the plan built with them.
SELECT sum(l_price) FROM t_remember_unmatched_l, t_remember_unmatched_o
WHERE o_orderkey = l_orderkey AND o_total = 5
  AND l_discount < (SELECT 0.2 * avg(l_discount) FROM t_remember_unmatched_l WHERE l_orderkey = o_orderkey)
FORMAT Null SETTINGS log_comment = 'remember_unmatched_correlated_1';
SELECT sum(l_price) FROM t_remember_unmatched_l, t_remember_unmatched_o
WHERE o_orderkey = l_orderkey AND o_total = 5
  AND l_discount < (SELECT 0.2 * avg(l_discount) FROM t_remember_unmatched_l WHERE l_orderkey = o_orderkey)
FORMAT Null SETTINGS log_comment = 'remember_unmatched_correlated_2';
SELECT sum(l_price) FROM t_remember_unmatched_l, t_remember_unmatched_o
WHERE o_orderkey = l_orderkey AND o_total = 5
  AND l_discount < (SELECT 0.2 * avg(l_discount) FROM t_remember_unmatched_l WHERE l_orderkey = o_orderkey)
FORMAT Null SETTINGS log_comment = 'remember_unmatched_correlated_3';

-- `LIMIT` without `ORDER BY`: the plans cannot be compared.
SELECT l_orderkey, l_price FROM t_remember_unmatched_l WHERE l_price > 10 LIMIT 10
FORMAT Null SETTINGS log_comment = 'remember_unmatched_limit_1';
SELECT l_orderkey, l_price FROM t_remember_unmatched_l WHERE l_price > 10 LIMIT 10
FORMAT Null SETTINGS log_comment = 'remember_unmatched_limit_2';

-- With `automatic_parallel_replicas_remember_boundary` off, nothing is remembered. The setting is part of the key the
-- verdict is remembered under, so both executions are off.
SELECT l_orderkey, l_price FROM t_remember_unmatched_l WHERE l_price > 10 LIMIT 10
FORMAT Null SETTINGS log_comment = 'remember_unmatched_limit_off_1', automatic_parallel_replicas_remember_boundary = 0;
SELECT l_orderkey, l_price FROM t_remember_unmatched_l WHERE l_price > 10 LIMIT 10
FORMAT Null SETTINGS log_comment = 'remember_unmatched_limit_off_2', automatic_parallel_replicas_remember_boundary = 0;

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment,
       ProfileEvents['AutomaticParallelReplicasProbePlansBuilt'] AS built,
       ProfileEvents['AutomaticParallelReplicasDecidedWithoutBuildingPlan'] AS remembered,
       ProfileEvents['AutoParallelReplicasPlanWithoutParallelReplicas'] AS without_parallel_replicas,
       ProfileEvents['AutoParallelReplicasPlansNotComparable'] AS not_comparable
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - toIntervalMinute(15)
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  -- `remember_unmatched_` cannot collide with the comment the test harness sets, which starts with the test name
  AND log_comment LIKE 'remember_unmatched_%'
  -- the replicas run sub-queries under the same comment; keep only the initiating query
  AND query_id = initial_query_id
ORDER BY log_comment;

DROP TABLE t_remember_unmatched_l;
DROP TABLE t_remember_unmatched_o;
