-- When parallel replicas receive the query as text (`serialize_query_plan = 1` without a local plan), every
-- replica applies `additional_table_filters` again, so an entry keyed by an alias or a bare table name is refused
-- for every table of the query, including tables read by subqueries, each under the map of its own subquery.

SET automatic_parallel_replicas_mode = 0, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_allow_in_with_subquery = 1,
    parallel_replicas_plan_based = 0;

DROP TABLE IF EXISTS t_outer;
DROP TABLE IF EXISTS t_inner;
CREATE TABLE t_outer (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_inner (y UInt64) ENGINE = MergeTree ORDER BY y;
INSERT INTO t_outer SELECT number FROM numbers(10);
INSERT INTO t_inner SELECT number FROM numbers(10);

-- A table read by an IN subquery.
SELECT count() FROM t_outer WHERE x IN (SELECT y FROM t_inner AS a)
SETTINGS additional_table_filters = {'a': 'y <= 2'}, enable_parallel_replicas = 2,
    serialize_query_plan = 1, parallel_replicas_local_plan = 0; -- { serverError SUPPORT_IS_DISABLED }
SELECT count() FROM t_outer WHERE x IN (SELECT y FROM t_inner AS a)
SETTINGS additional_table_filters = {'a': 'y <= 2'}, enable_parallel_replicas = 1,
    serialize_query_plan = 1, parallel_replicas_local_plan = 0;
SELECT count() FROM t_outer WHERE x IN (SELECT y FROM t_inner)
SETTINGS additional_table_filters = {'t_inner': 'y <= 2'}, enable_parallel_replicas = 2,
    serialize_query_plan = 1, parallel_replicas_local_plan = 0; -- { serverError SUPPORT_IS_DISABLED }
SELECT count() FROM (SELECT x FROM t_outer WHERE x IN (SELECT y FROM t_inner AS a))
SETTINGS additional_table_filters = {'a': 'y <= 2'}, enable_parallel_replicas = 2,
    serialize_query_plan = 1, parallel_replicas_local_plan = 0; -- { serverError SUPPORT_IS_DISABLED }
SELECT count() FROM t_outer WHERE (x IN (SELECT y FROM t_inner AS a)) GLOBAL IN (SELECT 1)
SETTINGS additional_table_filters = {'a': 'y <= 2'}, enable_parallel_replicas = 2,
    serialize_query_plan = 1, parallel_replicas_local_plan = 0; -- { serverError SUPPORT_IS_DISABLED }

-- A full name is matched alike on every replica, and the second argument of GLOBAL IN is read on the initiator.
SELECT count() FROM t_outer WHERE x IN (SELECT dummy FROM system.one)
SETTINGS additional_table_filters = {'system.one': 'dummy = 1'}, enable_parallel_replicas = 2,
    serialize_query_plan = 1, parallel_replicas_local_plan = 0;
SELECT count() FROM t_outer WHERE x GLOBAL IN (SELECT y FROM t_inner AS a)
SETTINGS additional_table_filters = {'a': 'y <= 2'}, enable_parallel_replicas = 1,
    serialize_query_plan = 1, parallel_replicas_local_plan = 0, log_comment = '05473_global_in';

-- A subquery that sets its own map.
SELECT count() FROM t_outer WHERE x IN (SELECT y FROM t_inner AS a SETTINGS additional_table_filters = {'a': 'y <= 2'})
SETTINGS enable_parallel_replicas = 2, serialize_query_plan = 1, parallel_replicas_local_plan = 0; -- { serverError SUPPORT_IS_DISABLED }
SELECT count() FROM t_outer WHERE x IN (SELECT y FROM t_inner AS a SETTINGS additional_table_filters = {'a': 'y <= 2'})
SETTINGS enable_parallel_replicas = 2, serialize_query_plan = 0, parallel_replicas_local_plan = 1; -- { serverError SUPPORT_IS_DISABLED }
SELECT count(), sum(y) FROM (SELECT y FROM t_inner AS a SETTINGS additional_table_filters = {'a': 'y <= 2'})
SETTINGS enable_parallel_replicas = 2, serialize_query_plan = 1, parallel_replicas_local_plan = 0; -- { serverError SUPPORT_IS_DISABLED }
SELECT count(), sum(y) FROM (SELECT y FROM t_inner AS a SETTINGS additional_table_filters = {'a': 'y <= 2'})
SETTINGS enable_parallel_replicas = 1, serialize_query_plan = 1, parallel_replicas_local_plan = 0;
-- A subquery that clears the map has no filter to lose.
SELECT count(), sum(y) FROM (SELECT y FROM t_inner AS a SETTINGS additional_table_filters = {})
SETTINGS additional_table_filters = {'a': 'y <= 2'}, enable_parallel_replicas = 2,
    serialize_query_plan = 1, parallel_replicas_local_plan = 0, log_comment = '05473_cleared_map';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, argMax(ProfileEvents['ParallelReplicasUsedCount'], event_time_microseconds) > 0
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND is_initial_query AND current_database = currentDatabase()
    AND log_comment IN ('05473_global_in', '05473_cleared_map')
GROUP BY log_comment
ORDER BY log_comment
SETTINGS enable_parallel_replicas = 0;

DROP TABLE t_outer;
DROP TABLE t_inner;
