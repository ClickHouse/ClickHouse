-- A correlated subquery turns parallel replicas off in the query (`DisableParallelReplicasPass`). When it turns them off
-- everywhere, automatic parallel replicas does not build the plan with parallel replicas, which would come out as the
-- single-node plan; without this every execution of such a query paid for that build.

DROP VIEW IF EXISTS v_skip_correlated;
DROP TABLE IF EXISTS t_skip_correlated_l;
DROP TABLE IF EXISTS t_skip_correlated_o;

CREATE TABLE t_skip_correlated_l (l_orderkey UInt64, l_price Float64, l_discount Float64) ENGINE = MergeTree ORDER BY l_orderkey;
CREATE TABLE t_skip_correlated_o (o_orderkey UInt64, o_total Float64) ENGINE = MergeTree ORDER BY o_orderkey;
INSERT INTO t_skip_correlated_l SELECT number % 10000, number % 1000, (number % 10) / 100 FROM numbers(100000);
INSERT INTO t_skip_correlated_o SELECT number, number % 1000 FROM numbers(10000);
CREATE VIEW v_skip_correlated SQL SECURITY INVOKER AS
    SELECT l_orderkey, sum(l_price) AS s FROM t_skip_correlated_l GROUP BY l_orderkey SETTINGS enable_parallel_replicas = 1;

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1, parallel_replicas_local_plan = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_for_queries_with_multiple_tables = 1,
    parallel_replicas_plan_based = 0, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    automatic_parallel_replicas_min_bytes_per_replica = 0, serialize_query_plan = 0;

SELECT sum(l_price) FROM t_skip_correlated_l, t_skip_correlated_o
WHERE o_orderkey = l_orderkey AND o_total = 5
  AND l_discount < (SELECT 0.2 * avg(l_discount) FROM t_skip_correlated_l WHERE l_orderkey = o_orderkey)
FORMAT Null SETTINGS log_comment = 'skip_correlated_1';
SELECT sum(l_price) FROM t_skip_correlated_l, t_skip_correlated_o
WHERE o_orderkey = l_orderkey AND o_total = 5
  AND l_discount < (SELECT 0.2 * avg(l_discount) FROM t_skip_correlated_l WHERE l_orderkey = o_orderkey)
FORMAT Null SETTINGS log_comment = 'skip_correlated_2';

-- `EXISTS` is correlated too.
SELECT count() FROM t_skip_correlated_o
WHERE o_total = 5 AND exists((SELECT 1 FROM t_skip_correlated_l WHERE l_orderkey = o_orderkey AND l_discount > 0.05))
FORMAT Null SETTINGS log_comment = 'skip_correlated_exists';

-- The subquery in `FROM` is planned before the correlated one is found, and keeps parallel replicas.
SELECT sum(l_price) FROM (SELECT l_orderkey, l_price, l_discount FROM t_skip_correlated_l WHERE l_price > 10) AS s, t_skip_correlated_o
WHERE o_orderkey = l_orderkey AND o_total = 5
  AND l_discount < (SELECT 0.2 * avg(l_discount) FROM t_skip_correlated_l WHERE l_orderkey = o_orderkey)
FORMAT Null SETTINGS log_comment = 'skip_correlated_kept_in_from';

-- A view plans its body with its own settings, which turn parallel replicas back on.
SELECT sum(s) FROM v_skip_correlated, t_skip_correlated_o
WHERE o_orderkey = l_orderkey AND o_total = 5
  AND s > (SELECT avg(l_price) FROM t_skip_correlated_l WHERE l_orderkey = o_orderkey)
FORMAT Null SETTINGS log_comment = 'skip_correlated_view';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment,
       ProfileEvents['AutomaticParallelReplicasProbePlansBuilt'] AS built,
       ProfileEvents['AutoParallelReplicasSkippedDueToQuery'] AS skipped
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - toIntervalMinute(15)
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  -- `skip_correlated_` cannot collide with the comment the test harness sets, which starts with the test name
  AND log_comment LIKE 'skip_correlated_%'
  -- the replicas run sub-queries under the same comment; keep only the initiating query
  AND query_id = initial_query_id
ORDER BY log_comment;

DROP VIEW v_skip_correlated;
DROP TABLE t_skip_correlated_l;
DROP TABLE t_skip_correlated_o;
