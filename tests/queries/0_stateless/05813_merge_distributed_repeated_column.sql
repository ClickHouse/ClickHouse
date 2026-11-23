-- Tags: distributed
-- A Merge read over a Distributed table must keep a column the query selects twice.

DROP TABLE IF EXISTS t_dist_two_shards;
DROP TABLE IF EXISTS t_dist_one_shard;
DROP TABLE IF EXISTS t_local;
DROP TABLE IF EXISTS t_other;

CREATE TABLE t_local (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_dist_two_shards AS t_local ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t_local);
CREATE TABLE t_dist_one_shard AS t_local ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_local);
INSERT INTO t_local VALUES (1, 10);
CREATE TABLE t_other (k UInt32, v UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_other VALUES (1, 100);

SELECT k, k FROM merge(currentDatabase(), '^t_dist_one_shard$') FORMAT TSVWithNames;

-- A filter on the second copy, pushed down into the Merge read.
SELECT a FROM (SELECT k AS a, k AS b FROM merge(currentDatabase(), '^t_(dist_one_shard|other)$')) WHERE b = 1;

DROP TABLE t_dist_two_shards;
DROP TABLE t_dist_one_shard;
DROP TABLE t_local;
DROP TABLE t_other;
