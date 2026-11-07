-- Tags: shard

-- `count` of a `Nullable` column and `distinctJSONPaths` over a `Merge` read that has a `Distributed` child and a local child.

DROP TABLE IF EXISTS t_merge;
DROP TABLE IF EXISTS t_dist;
DROP TABLE IF EXISTS t_local;
DROP TABLE IF EXISTS tj_dist;
DROP TABLE IF EXISTS tj_local;
DROP VIEW IF EXISTS v_shard;

CREATE TABLE t_local (k UInt8, x Nullable(UInt8)) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_dist AS t_local ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), t_local);
CREATE TABLE t_merge AS t_local ENGINE = Merge(currentDatabase(), '^t_(local|dist)$');
INSERT INTO t_local VALUES (1, 1), (2, NULL);

SELECT count(x), count() FROM merge(currentDatabase(), '^t_(local|dist)$') SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count(x), count() FROM t_merge SETTINGS optimize_functions_to_subcolumns = 1;
SELECT k, count(x) FROM t_merge GROUP BY k ORDER BY k SETTINGS optimize_functions_to_subcolumns = 1;

CREATE TABLE tj_local (k UInt8, j JSON) ENGINE = MergeTree ORDER BY k;
CREATE TABLE tj_dist AS tj_local ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), tj_local);
INSERT INTO tj_local VALUES (1, '{"a":1}'), (2, '{"b":{"c":2}}');

SELECT distinctJSONPaths(j) FROM merge(currentDatabase(), '^tj_(local|dist)$') SETTINGS optimize_functions_to_subcolumns = 1;

-- A view child inlined during the child analysis reads `_shard_num` of a `Distributed` table: it must still be rewritten to `shardNum`.
CREATE VIEW v_shard AS SELECT k, toNullable(toUInt8(_shard_num)) AS x FROM t_dist;
SELECT count(x), count() FROM merge(currentDatabase(), '^(v_shard|t_dist)$') SETTINGS analyzer_inline_views = 1;

DROP VIEW v_shard;
DROP TABLE t_merge;
DROP TABLE t_dist;
DROP TABLE t_local;
DROP TABLE tj_dist;
DROP TABLE tj_local;
