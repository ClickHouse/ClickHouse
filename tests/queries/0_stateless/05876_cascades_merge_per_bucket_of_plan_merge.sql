-- A merge of aggregation states that the query plan already has gets the merge per bucket as an alternative
-- to the merge on one node, the same as the merge half of a two-stage aggregation. The plan of a query over a
-- cluster with local shards has such a merge: the plans of the shards are below it.

DROP TABLE IF EXISTS t_plan_merge;
CREATE TABLE t_plan_merge (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';
-- a merge between planning and the read would invalidate the planned part names
SYSTEM STOP MERGES t_plan_merge;
INSERT INTO t_plan_merge SELECT number % 1000, number FROM numbers(20000);

SET explain_query_plan_default = 'legacy';
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET max_rows_to_group_by = 0;
SET use_statistics = 0;
SET prefer_localhost_replica = 1;
SET distributed_aggregation_memory_efficient = 1;
SET enable_memory_bound_merging_of_aggregation_results = 0;
SET distributed_plan_partial_aggregation_before_shuffle = 1;
SET distributed_plan_force_shuffle_aggregation = 0;
SET param__internal_cascades_cluster_node_count = 4;
-- Many rows and many groups, a cheap network and no price for the rows of a gather or a scatter: the merge per
-- bucket costs about two thirds of the merge on one node.
SET param__internal_join_table_stat_hints = '{"t_plan_merge": {"cardinality": 1000000000, "avg_row_bytes": 16, "distinct_keys": {"k": 500000000}}}';
SET param__internal_cascades_cost_config = '{"network_weight": 0.01, "funnel_sequential_cost_per_row": 0}';

SELECT '-- the merge per bucket over the states of the two shards';
EXPLAIN SELECT k, sum(v) FROM cluster('test_cluster_two_shards_localhost', currentDatabase(), t_plan_merge) GROUP BY k;

SELECT '-- the result, and the result without the distributed plan';
SELECT count(), sum(s), uniqExact(k) FROM (SELECT k, sum(v) AS s FROM cluster('test_cluster_two_shards_localhost', currentDatabase(), t_plan_merge) GROUP BY k);
SELECT count(), sum(s), uniqExact(k) FROM (SELECT k, sum(v) AS s FROM cluster('test_cluster_two_shards_localhost', currentDatabase(), t_plan_merge) GROUP BY k)
SETTINGS make_distributed_plan = 0;

SELECT '-- the same result with two-level states from the shards';
SELECT count(), sum(s), uniqExact(k) FROM (SELECT k, sum(v) AS s FROM cluster('test_cluster_two_shards_localhost', currentDatabase(), t_plan_merge) GROUP BY k)
SETTINGS group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1;

SELECT '-- distributed_plan_force_shuffle_aggregation asks for no shuffle of states: the merge is on one node';
EXPLAIN SELECT k, sum(v) FROM cluster('test_cluster_two_shards_localhost', currentDatabase(), t_plan_merge) GROUP BY k
SETTINGS distributed_plan_force_shuffle_aggregation = 1;

DROP TABLE t_plan_merge;
