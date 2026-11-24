-- Tags: distributed, no-parallel
-- The cluster uses the shared `shard_0` and `shard_1` databases.

SET in_null_as_unknown = 1;

CREATE DATABASE IF NOT EXISTS shard_0;
CREATE DATABASE IF NOT EXISTS shard_1;
DROP TABLE IF EXISTS shard_0.squality_shard_rewrite_null;
DROP TABLE IF EXISTS shard_1.squality_shard_rewrite_null;
DROP TABLE IF EXISTS squality_shard_rewrite_null_dist;

CREATE TABLE shard_0.squality_shard_rewrite_null (k UInt64) ENGINE = Memory;
CREATE TABLE shard_1.squality_shard_rewrite_null (k UInt64) ENGINE = Memory;
INSERT INTO shard_0.squality_shard_rewrite_null VALUES (0), (2);
INSERT INTO shard_1.squality_shard_rewrite_null VALUES (1);
CREATE TABLE squality_shard_rewrite_null_dist (k UInt64)
ENGINE = Distributed('test_cluster_two_shards_different_databases', '', squality_shard_rewrite_null, k);

SELECT k FROM squality_shard_rewrite_null_dist AS d
WHERE k IN (0, 1, 2) AND isNull(k IN (0, NULL))
ORDER BY k
SETTINGS optimize_skip_unused_shards = 1, optimize_skip_unused_shards_rewrite_in = 0;

SELECT k FROM squality_shard_rewrite_null_dist AS d
WHERE k IN (0, 1, 2) AND isNull(k IN (0, NULL))
ORDER BY k
SETTINGS optimize_skip_unused_shards = 1, optimize_skip_unused_shards_rewrite_in = 1;

DROP TABLE squality_shard_rewrite_null_dist;
DROP TABLE shard_0.squality_shard_rewrite_null;
DROP TABLE shard_1.squality_shard_rewrite_null;
