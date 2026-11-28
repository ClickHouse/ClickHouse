-- Tags: shard

DROP TABLE IF EXISTS t_shardnum_local;
DROP TABLE IF EXISTS t_shardnum_dist;

CREATE TABLE t_shardnum_local (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_shardnum_dist AS t_shardnum_local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t_shardnum_local);
INSERT INTO t_shardnum_local SELECT number % 4 AS k, number AS v FROM numbers(20);

-- A query that is not distributed returns the constant 0.
SELECT shardNum() AS s, shardCount() AS c;

-- `serialize_query_plan = 0` re-plans on the shard and folds `shardNum`.
SELECT shardNum() AS s, shardCount() AS c, count() FROM t_shardnum_dist GROUP BY s, c ORDER BY s, c
SETTINGS make_distributed_plan = 1, serialize_query_plan = 0, enable_parallel_replicas = 0;

-- The serialized per-shard plan must keep the shard index, including when stateless workers run the fragments.
SELECT shardNum() AS s, shardCount() AS c, count() FROM t_shardnum_dist GROUP BY s, c ORDER BY s, c
SETTINGS make_distributed_plan = 1, serialize_query_plan = 1, enable_parallel_replicas = 0;

-- Both shards are remote, so neither result can come from the in-process localhost plan.
SELECT shardNum() AS s, shardCount() AS c, count() FROM t_shardnum_dist GROUP BY s, c ORDER BY s, c
SETTINGS make_distributed_plan = 1, serialize_query_plan = 1, prefer_localhost_replica = 0, enable_parallel_replicas = 0;

-- In-process distributed tasks copy the shard context. The folded index must still be 1 and 2.
SELECT shardNum() AS s, shardCount() AS c, count() FROM t_shardnum_dist GROUP BY s, c ORDER BY s, c
SETTINGS make_distributed_plan = 1, serialize_query_plan = 1, distributed_plan_execute_locally = 1, enable_parallel_replicas = 0;

-- The shard executes the deserialized plan itself.
SELECT shardNum() AS s, shardCount() AS c, count() FROM t_shardnum_dist GROUP BY s, c ORDER BY s, c
SETTINGS make_distributed_plan = 0, serialize_query_plan = 1, enable_parallel_replicas = 0;

DROP TABLE t_shardnum_dist;
DROP TABLE t_shardnum_local;
