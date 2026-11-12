-- Tags: distributed, no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- A Merge child that references an outer subquery set cannot ship it to a distributed-plan worker
-- task, so ReadFromMerge::createChildrenPlans clears make_distributed_plan on that child's own copy
-- of the context (queryHasSubquerySets) while an explicit distributed_plan_workers_num on the
-- session is still inherited untouched. When that child is a Distributed table, its shard dispatch
-- (ClusterProxy) carries both settings to the secondary query it sends - which must not be rejected
-- as a conflict: the want/refuse check only applies to the query the client actually asked for.

DROP TABLE IF EXISTS t_secondary_local;
DROP TABLE IF EXISTS t_secondary_dist;
CREATE TABLE t_secondary_local (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_secondary_dist AS t_secondary_local ENGINE = Distributed(test_shard_localhost, currentDatabase(), 't_secondary_local');
INSERT INTO t_secondary_local SELECT number FROM numbers(10);

SET distributed_plan_execute_locally = 1, distributed_plan_workers_num = 3;

SELECT count() FROM merge(currentDatabase(), '^t_secondary_dist$') WHERE x IN (SELECT number FROM numbers(5));

DROP TABLE t_secondary_dist;
DROP TABLE t_secondary_local;
