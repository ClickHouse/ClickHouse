-- Tags: shard
-- A correlated subquery in a view body must not be read with parallel replicas, also when the body is
-- expanded on a remote shard (a parallel replicas initiator for its own replicas) or on every replica.

DROP VIEW IF EXISTS v_corr;
DROP TABLE IF EXISTS t_corr;

CREATE TABLE t_corr (v UInt64) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_corr SELECT number FROM numbers(1000);
CREATE VIEW v_corr AS SELECT v FROM t_corr WHERE (SELECT v + 1) > 5;

-- Without the in-memory buffer each replica decorrelates the body into two reads of the same table.
SET correlated_subqueries_use_in_memory_buffer = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET prefer_localhost_replica = 0, parallel_replicas_plan_based = 0, automatic_parallel_replicas_mode = 0, serialize_query_plan = 0;

SELECT count() FROM cluster('test_cluster_one_shard_two_replicas', view(SELECT v FROM {CLICKHOUSE_DATABASE:Identifier}.t_corr WHERE (SELECT v + 1) > 5));
SELECT count() FROM cluster('test_cluster_one_shard_two_replicas', currentDatabase(), v_corr);
SELECT count() FROM v_corr SETTINGS parallel_replicas_allow_view_over_mergetree = 1;

DROP VIEW v_corr;
DROP TABLE t_corr;
