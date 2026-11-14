-- Tags: no-parallel, no-random-settings
-- `shard_0` and `shard_1` are shared with other tests, and row policies are global.

-- A `Distributed` table without a database argument resolves its source table in the configured default
-- database of each shard. The plan-level read of distributed lazy materialization skips the planning of
-- the source table, so a row policy or an `ALIAS` column of a source table in those databases keeps the
-- classic path. The results must match the classic path.

SET serialize_query_plan = 1;
SET query_plan_optimize_distributed_lazy_materialization = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET distributed_push_down_limit = 1;
SET skip_unavailable_shards = 0;
SET enable_parallel_replicas = 0;
SET explain_query_plan_default = 'legacy';

CREATE DATABASE IF NOT EXISTS shard_0;
CREATE DATABASE IF NOT EXISTS shard_1;

DROP ROW POLICY IF EXISTS dlm_05622_policy ON shard_0.dlm_05622_src, shard_1.dlm_05622_src;
DROP TABLE IF EXISTS dlm_05622_dist;
DROP TABLE IF EXISTS dlm_05622_alias_dist;
DROP TABLE IF EXISTS shard_0.dlm_05622_src;
DROP TABLE IF EXISTS shard_1.dlm_05622_src;
DROP TABLE IF EXISTS shard_0.dlm_05622_alias;
DROP TABLE IF EXISTS shard_1.dlm_05622_alias;

SELECT '-- Row policy on a Memory source in the default database of each shard';
CREATE TABLE shard_0.dlm_05622_src (x UInt64) ENGINE = Memory;
CREATE TABLE shard_1.dlm_05622_src (x UInt64) ENGINE = Memory;
INSERT INTO shard_0.dlm_05622_src VALUES (0), (1), (2), (3);
INSERT INTO shard_1.dlm_05622_src VALUES (0), (1), (2), (3);
CREATE ROW POLICY dlm_05622_policy ON shard_0.dlm_05622_src, shard_1.dlm_05622_src USING x % 2 = 0 TO ALL;
CREATE TABLE dlm_05622_dist (x UInt64) ENGINE = Distributed(test_cluster_two_shards_different_databases, '', dlm_05622_src);
SELECT count() FROM (EXPLAIN actions = 1, distributed = 1 SELECT x FROM dlm_05622_dist ORDER BY x LIMIT 4) WHERE explain LIKE '%Distributed TopK candidate limit%';
SELECT x FROM dlm_05622_dist ORDER BY x LIMIT 4;

SELECT '-- ALIAS column of a source in the default database of each shard';
CREATE TABLE shard_0.dlm_05622_alias (x UInt64, y UInt64 ALIAS x + 1) ENGINE = MergeTree ORDER BY x;
CREATE TABLE shard_1.dlm_05622_alias (x UInt64, y UInt64 ALIAS x + 1) ENGINE = MergeTree ORDER BY x;
INSERT INTO shard_0.dlm_05622_alias SELECT number FROM numbers(5);
INSERT INTO shard_1.dlm_05622_alias SELECT number FROM numbers(5);
CREATE TABLE dlm_05622_alias_dist (y UInt64) ENGINE = Distributed(test_cluster_two_shards_different_databases, '', dlm_05622_alias);
SELECT count() FROM (EXPLAIN actions = 1, distributed = 1 SELECT y FROM dlm_05622_alias_dist ORDER BY y LIMIT 3) WHERE explain LIKE '%Distributed TopK candidate limit%';
SELECT y FROM dlm_05622_alias_dist ORDER BY y LIMIT 3;

DROP ROW POLICY dlm_05622_policy ON shard_0.dlm_05622_src, shard_1.dlm_05622_src;
DROP TABLE dlm_05622_alias_dist;
DROP TABLE dlm_05622_dist;
DROP TABLE shard_0.dlm_05622_alias;
DROP TABLE shard_1.dlm_05622_alias;
DROP TABLE shard_0.dlm_05622_src;
DROP TABLE shard_1.dlm_05622_src;
