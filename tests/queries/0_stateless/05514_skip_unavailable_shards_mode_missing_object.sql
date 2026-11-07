-- Tags: no-parallel, shard
-- no-parallel: the cluster below resolves its shards in the global databases `shard_0` / `shard_1`.

-- `skip_unavailable_shards_mode = 'unavailable_or_table_missing'` (the default) treats a shard as
-- unavailable when the Distributed table's own remote table (or its database) does not exist there.
-- That is decided while the connection is established, before the query is sent. It must not extend
-- to an `UNKNOWN_TABLE` / `UNKNOWN_DATABASE` error that the shard raises while analysing or executing
-- the query, because such an error can name any object the query touches (a table behind a view, a
-- table of a non-GLOBAL `IN` subquery, a dictionary, ...) on a shard that is up and does hold the
-- target table: skipping it silently turns a wrong query into a wrong result.

DROP DATABASE IF EXISTS shard_0;
DROP DATABASE IF EXISTS shard_1;
DROP DATABASE IF EXISTS db_05514_aux;
CREATE DATABASE shard_0;
CREATE DATABASE shard_1;
CREATE DATABASE db_05514_aux;

CREATE TABLE shard_0.t_05514 (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO shard_0.t_05514 VALUES (0), (2);

-- On shard_1 the target is a view; the objects it reads from are dropped below, so the shard accepts
-- the connection (the target exists) and then fails the query during analysis.
CREATE TABLE shard_1.src_05514 (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE db_05514_aux.src_05514 (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO shard_1.src_05514 VALUES (1), (3);
CREATE VIEW shard_1.t_05514 AS SELECT x FROM shard_1.src_05514;
CREATE VIEW shard_1.t_05514_db AS SELECT x FROM db_05514_aux.src_05514;
CREATE VIEW shard_0.t_05514_db AS SELECT x FROM shard_0.t_05514;

DROP TABLE IF EXISTS dist_05514;
DROP TABLE IF EXISTS dist_05514_db;
CREATE TABLE dist_05514 (x UInt32) ENGINE = Distributed(test_cluster_two_shards_different_databases, '', t_05514);
CREATE TABLE dist_05514_db (x UInt32) ENGINE = Distributed(test_cluster_two_shards_different_databases, '', t_05514_db);

SET prefer_localhost_replica = 0;

SELECT 'all present', count() FROM dist_05514;

DROP TABLE shard_1.src_05514;
DROP DATABASE db_05514_aux;

-- shard_1 is up and holds `t_05514`, but reading it raises UNKNOWN_TABLE there.
SELECT count() FROM dist_05514; -- { serverError UNKNOWN_TABLE }
SELECT count() FROM dist_05514 SETTINGS skip_unavailable_shards = 1; -- { serverError UNKNOWN_TABLE }
SELECT count() FROM dist_05514 SETTINGS skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable_or_table_missing'; -- { serverError UNKNOWN_TABLE }
SELECT count() FROM dist_05514 SETTINGS skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable'; -- { serverError UNKNOWN_TABLE }
-- Only the mode that tolerates any exception raised before the shard returned data skips it.
SELECT 'tolerant', count() FROM dist_05514 SETTINGS skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable_or_exception_before_processing';

-- Same with UNKNOWN_DATABASE: the database the view reads from is gone, the shard's own database is not.
SELECT count() FROM dist_05514_db SETTINGS skip_unavailable_shards = 1; -- { serverError UNKNOWN_DATABASE }
SELECT 'tolerant db', count() FROM dist_05514_db SETTINGS skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable_or_exception_before_processing';

-- An `INSERT SELECT` that each shard runs on its own data does not check the target in advance, so a shard
-- without the target table is still skipped and the other shard is written.
CREATE TABLE shard_0.ins_src_05514 (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE shard_1.ins_src_05514 (x UInt32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE shard_0.ins_dst_05514 (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO shard_0.ins_src_05514 VALUES (10);
INSERT INTO shard_1.ins_src_05514 VALUES (11);
DROP TABLE IF EXISTS dist_ins_src_05514;
DROP TABLE IF EXISTS dist_ins_dst_05514;
CREATE TABLE dist_ins_src_05514 (x UInt32) ENGINE = Distributed(test_cluster_two_shards_different_databases, '', ins_src_05514);
CREATE TABLE dist_ins_dst_05514 (x UInt32) ENGINE = Distributed(test_cluster_two_shards_different_databases, '', ins_dst_05514);
INSERT INTO dist_ins_dst_05514 SELECT * FROM dist_ins_src_05514
SETTINGS parallel_distributed_insert_select = 2, skip_unavailable_shards = 1;
SELECT 'insert select, target missing on one shard', groupArray(x) FROM shard_0.ins_dst_05514;
DROP TABLE dist_ins_src_05514;
DROP TABLE dist_ins_dst_05514;

-- The engine-level setting behaves the same way.
DROP TABLE IF EXISTS dist_05514_engine;
CREATE TABLE dist_05514_engine (x UInt32) ENGINE = Distributed(test_cluster_two_shards_different_databases, '', t_05514)
SETTINGS skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable_or_table_missing';
SELECT count() FROM dist_05514_engine; -- { serverError UNKNOWN_TABLE }

-- A shard where the target table itself, or its database, is missing is still skipped by default:
-- that is the historical meaning of "table missing".
DROP TABLE shard_1.t_05514;
SELECT 'target table missing', count() FROM dist_05514 SETTINGS skip_unavailable_shards = 1;
SELECT 'target table missing, engine', count() FROM dist_05514_engine;
DROP DATABASE shard_1;
SELECT 'target database missing', count() FROM dist_05514 SETTINGS skip_unavailable_shards = 1;
SELECT count() FROM dist_05514; -- { serverError ALL_CONNECTION_TRIES_FAILED }

DROP TABLE dist_05514;
DROP TABLE dist_05514_db;
DROP TABLE dist_05514_engine;
DROP DATABASE shard_0;
