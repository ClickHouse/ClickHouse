-- Tags: shard, no-parallel-replicas, no-random-merge-tree-settings
-- A grouping-key conjunct of `HAVING` moved to `WHERE` on a shard must not reduce the rows seen by a stateful function
-- or a function depending on the block in the join tree, like in `ARRAY JOIN` or `JOIN ON`: the plan optimizer may push
-- the new `WHERE` below them.

SET enable_analyzer = 1;
SET prefer_localhost_replica = 0;
SET serialize_query_plan = 0;

DROP TABLE IF EXISTS t_src_05448;
DROP TABLE IF EXISTS t_dist_05448;

-- The table is not sorted by the key, so the rows of one group are spread over the block.
CREATE TABLE t_src_05448 (k UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_dist_05448 AS t_src_05448
    ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), 't_src_05448');
INSERT INTO t_src_05448 SELECT number % 10 FROM numbers(100);

SELECT '--- ARRAY JOIN';
SELECT k, count() FROM t_dist_05448 ARRAY JOIN range(blockSize()) AS v GROUP BY k HAVING k = 0
    SETTINGS max_threads = 1, max_block_size = 100;
SELECT k, count() FROM t_dist_05448 ARRAY JOIN range(blockSize()) AS v GROUP BY k HAVING k = 0
    SETTINGS max_threads = 1, max_block_size = 100, query_plan_enable_optimizations = 0;

SELECT '--- JOIN ON';
SELECT a.k, count() FROM t_dist_05448 AS a INNER JOIN t_src_05448 AS b ON a.k + rowNumberInAllBlocks() = b.k
    GROUP BY a.k HAVING a.k = 0
    SETTINGS max_threads = 1, max_block_size = 100;
SELECT a.k, count() FROM t_dist_05448 AS a INNER JOIN t_src_05448 AS b ON a.k + rowNumberInAllBlocks() = b.k
    GROUP BY a.k HAVING a.k = 0
    SETTINGS max_threads = 1, max_block_size = 100, query_plan_enable_optimizations = 0;

DROP TABLE t_dist_05448;
DROP TABLE t_src_05448;
