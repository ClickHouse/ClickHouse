-- Tags: shard, no-parallel
-- Tag no-parallel: the shards of `test_cluster_two_shards_different_databases` read the databases `shard_0` and `shard_1`,
-- whose names are fixed.

-- A `Distributed` table whose declared column type does not preserve the order of the shard table's type is refused for
-- ORDER BY also when the shard replicas are this server with a `<default_database>`: such a replica is not `is_local`
-- (it is queried over TCP), but it reads its table on this server, so the check can and must see it.

CREATE DATABASE IF NOT EXISTS shard_0;
CREATE DATABASE IF NOT EXISTS shard_1;

DROP TABLE IF EXISTS dist_int8_05871;
DROP TABLE IF EXISTS dist_str_05871;
DROP TABLE IF EXISTS dist_nullable_05871;
DROP TABLE IF EXISTS dist_mixed_05871;
DROP TABLE IF EXISTS dist_explicit_05871;
DROP TABLE IF EXISTS t_str_05871;
DROP TABLE IF EXISTS shard_0.t_05871;
DROP TABLE IF EXISTS shard_1.t_05871;
DROP TABLE IF EXISTS shard_0.u_05871;
DROP TABLE IF EXISTS shard_1.u_05871;
DROP TABLE IF EXISTS shard_0.m_05871;
DROP TABLE IF EXISTS shard_1.m_05871;

-- The same values on both shards.
CREATE TABLE shard_0.t_05871 (s String) ENGINE = MergeTree ORDER BY s;
CREATE TABLE shard_1.t_05871 (s String) ENGINE = MergeTree ORDER BY s;
INSERT INTO shard_0.t_05871 SELECT toString(number % 21) FROM numbers(100);
INSERT INTO shard_1.t_05871 SELECT toString(number % 21) FROM numbers(100);

CREATE TABLE dist_int8_05871 (s Int8) ENGINE = Distributed(test_cluster_two_shards_different_databases, '', t_05871);

SELECT '-- String shard tables read as Int8';
SELECT DISTINCT s FROM dist_int8_05871 ORDER BY s DESC LIMIT 3 SETTINGS optimize_distinct_in_order = 1; -- { serverError INCOMPATIBLE_COLUMNS }
SELECT s FROM dist_int8_05871 ORDER BY s DESC LIMIT 3; -- { serverError INCOMPATIBLE_COLUMNS }
-- Without ORDER BY nothing relies on the shards' order.
SELECT count() FROM dist_int8_05871;

SELECT '-- UInt64 shard tables read as Nullable(String)';
CREATE TABLE shard_0.u_05871 (s UInt64) ENGINE = MergeTree ORDER BY s;
CREATE TABLE shard_1.u_05871 (s UInt64) ENGINE = MergeTree ORDER BY s;
INSERT INTO shard_0.u_05871 SELECT number FROM numbers(50);
INSERT INTO shard_1.u_05871 SELECT number FROM numbers(50);
CREATE TABLE dist_nullable_05871 (s Nullable(String)) ENGINE = Distributed(test_cluster_two_shards_different_databases, '', u_05871);
SELECT DISTINCT * FROM dist_nullable_05871 ORDER BY s ASC LIMIT 39, 5; -- { serverError INCOMPATIBLE_COLUMNS }

SELECT '-- only the table of the second shard has a mismatching type';
CREATE TABLE shard_0.m_05871 (s Int8) ENGINE = MergeTree ORDER BY s;
CREATE TABLE shard_1.m_05871 (s String) ENGINE = MergeTree ORDER BY s;
INSERT INTO shard_0.m_05871 SELECT number % 21 FROM numbers(100);
INSERT INTO shard_1.m_05871 SELECT toString(number % 21) FROM numbers(100);
CREATE TABLE dist_mixed_05871 (s Int8) ENGINE = Distributed(test_cluster_two_shards_different_databases, '', m_05871);
SELECT s FROM dist_mixed_05871 ORDER BY s DESC LIMIT 3; -- { serverError INCOMPATIBLE_COLUMNS }

SELECT '-- an explicit remote database';
CREATE TABLE t_str_05871 (s String) ENGINE = MergeTree ORDER BY s;
INSERT INTO t_str_05871 SELECT toString(number % 21) FROM numbers(100);
CREATE TABLE dist_explicit_05871 (s Int8) ENGINE = Distributed(test_cluster_two_shards_different_databases, currentDatabase(), t_str_05871);
SELECT s FROM dist_explicit_05871 ORDER BY s DESC LIMIT 3; -- { serverError INCOMPATIBLE_COLUMNS }

SELECT '-- the matching type still works';
CREATE TABLE dist_str_05871 (s String) ENGINE = Distributed(test_cluster_two_shards_different_databases, '', t_05871);
SELECT DISTINCT s FROM dist_str_05871 ORDER BY s DESC LIMIT 3;

DROP TABLE dist_int8_05871;
DROP TABLE dist_str_05871;
DROP TABLE dist_nullable_05871;
DROP TABLE dist_mixed_05871;
DROP TABLE dist_explicit_05871;
DROP TABLE t_str_05871;
DROP TABLE shard_0.t_05871;
DROP TABLE shard_1.t_05871;
DROP TABLE shard_0.u_05871;
DROP TABLE shard_1.u_05871;
DROP TABLE shard_0.m_05871;
DROP TABLE shard_1.m_05871;
