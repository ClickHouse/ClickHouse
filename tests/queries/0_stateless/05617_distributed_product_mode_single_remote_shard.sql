-- Tags: distributed

-- A `Distributed` table with a single shard was never rewritten by `distributed_product_mode`, on the
-- assumption that its only shard is local on every node. That does not hold when the shard has no local
-- replica: a non-GLOBAL subquery over such a table then reads the whole remote shard again on every shard
-- of the outer query. Such a table must be handled like a table with several shards.
-- `test_shard_non_local` has a single replica on 127.0.0.2, which is not considered local, so it
-- gives a single shard without a local replica.

DROP TABLE IF EXISTS t_local;
DROP TABLE IF EXISTS t_dist;
DROP TABLE IF EXISTS t_single_local;
DROP TABLE IF EXISTS t_single_remote;

CREATE TABLE t_local (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_local VALUES (1), (2), (3);

-- `rand()` as the sharding key, so that a randomized `optimize_skip_unused_shards` cannot prune a shard.
CREATE TABLE t_dist AS t_local ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t_local, rand());
CREATE TABLE t_single_local AS t_local ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_local);
CREATE TABLE t_single_remote AS t_local ENGINE = Distributed(test_shard_non_local, currentDatabase(), t_local);

SET enable_analyzer = 1;

SELECT 'single local shard is not rewritten';
SELECT x FROM t_dist WHERE x IN (SELECT x FROM t_single_local WHERE x < 3) ORDER BY x SETTINGS distributed_product_mode = 'deny';

SELECT 'single remote shard is rewritten';
SELECT x FROM t_dist WHERE x IN (SELECT x FROM t_single_remote WHERE x < 3) ORDER BY x SETTINGS distributed_product_mode = 'deny'; -- { serverError DISTRIBUTED_IN_JOIN_SUBQUERY_DENIED }
SELECT x FROM t_dist WHERE x IN (SELECT x FROM t_single_remote WHERE x < 3) ORDER BY x SETTINGS distributed_product_mode = 'global';
SELECT x FROM t_dist WHERE x IN (SELECT x FROM t_single_remote WHERE x < 3) ORDER BY x SETTINGS distributed_product_mode = 'local';
SELECT x FROM t_dist WHERE x IN (SELECT x FROM t_single_remote WHERE x < 3) ORDER BY x SETTINGS distributed_product_mode = 'allow';

SELECT 'JOIN with a single remote shard';
SELECT a.x, b.x FROM t_dist AS a JOIN t_single_remote AS b ON a.x = b.x ORDER BY a.x SETTINGS distributed_product_mode = 'deny'; -- { serverError DISTRIBUTED_IN_JOIN_SUBQUERY_DENIED }
SELECT a.x, b.x FROM t_dist AS a JOIN t_single_remote AS b ON a.x = b.x ORDER BY a.x SETTINGS distributed_product_mode = 'global';

SELECT 'old analyzer';
SET enable_analyzer = 0;
SELECT x FROM t_dist WHERE x IN (SELECT x FROM t_single_local WHERE x < 3) ORDER BY x SETTINGS distributed_product_mode = 'deny';
SELECT x FROM t_dist WHERE x IN (SELECT x FROM t_single_remote WHERE x < 3) ORDER BY x SETTINGS distributed_product_mode = 'deny'; -- { serverError DISTRIBUTED_IN_JOIN_SUBQUERY_DENIED }
SELECT x FROM t_dist WHERE x IN (SELECT x FROM t_single_remote WHERE x < 3) ORDER BY x SETTINGS distributed_product_mode = 'global';
SELECT x FROM t_single_remote WHERE x IN (SELECT x FROM t_single_remote WHERE x < 3) ORDER BY x SETTINGS distributed_product_mode = 'deny'; -- { serverError DISTRIBUTED_IN_JOIN_SUBQUERY_DENIED }

DROP TABLE t_single_remote;
DROP TABLE t_single_local;
DROP TABLE t_dist;
DROP TABLE t_local;
