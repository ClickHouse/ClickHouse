-- Tags: no-parallel, no-fasttest
-- - no-parallel, no-fasttest - due to usage of fail points

-- With parallel replicas, every replica must read in the same order, even when some of them use a projection and the others
-- read the table: the order of an aggregation is decided from the table. The failpoint makes the followers skip the projection.

DROP TABLE IF EXISTS t_projection_sorted_by_key;
CREATE TABLE t_projection_sorted_by_key (k UInt64, v UInt64, PROJECTION p (SELECT * ORDER BY k))
ENGINE = MergeTree ORDER BY v SETTINGS index_granularity = 128;
INSERT INTO t_projection_sorted_by_key SELECT number % 10, number FROM numbers(100000);

DROP TABLE IF EXISTS t_table_sorted_by_key;
CREATE TABLE t_table_sorted_by_key (k UInt64, v UInt64, PROJECTION p (SELECT * ORDER BY v))
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;
INSERT INTO t_table_sorted_by_key SELECT number % 10, number FROM numbers(100000);

SET enable_parallel_replicas = 1, max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_local_plan = 1,
    parallel_replicas_plan_based = 0, automatic_parallel_replicas_mode = 0, parallel_replicas_support_projection = 1,
    parallel_replicas_min_number_of_rows_per_replica = 0, optimize_use_projections = 1, optimize_aggregation_in_order = 1,
    max_threads = 2;

SYSTEM ENABLE FAILPOINT parallel_replicas_skip_aggregate_projection_on_follower;

SELECT sum(c) FROM (SELECT k, count() c FROM t_projection_sorted_by_key WHERE k < 9 GROUP BY k);
SELECT sum(c) FROM (SELECT k, count() c FROM t_table_sorted_by_key WHERE v < 90000 GROUP BY k);

SYSTEM DISABLE FAILPOINT parallel_replicas_skip_aggregate_projection_on_follower;

DROP TABLE t_projection_sorted_by_key;
DROP TABLE t_table_sorted_by_key;
