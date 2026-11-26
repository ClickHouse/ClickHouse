-- A Buffer over Distributed read above FetchColumns through a table that declares other column types than the Buffer.

DROP TABLE IF EXISTS t_05894_same;
DROP TABLE IF EXISTS t_05894_outer;
DROP TABLE IF EXISTS t_05894_rmv;
DROP TABLE IF EXISTS t_05894_mv;
DROP TABLE IF EXISTS t_05894_src;
DROP TABLE IF EXISTS t_05894_buf;
DROP TABLE IF EXISTS t_05894_dist;
DROP TABLE IF EXISTS t_05894_local;

CREATE TABLE t_05894_local (a UInt8) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_05894_local VALUES (1), (2);
CREATE TABLE t_05894_dist AS t_05894_local ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_05894_local);
CREATE TABLE t_05894_buf AS t_05894_local ENGINE = Buffer(currentDatabase(), t_05894_dist, 1, 100000, 100000, 1000000, 1000000, 100000000, 100000000);
INSERT INTO t_05894_buf VALUES (3);
CREATE TABLE t_05894_outer (a Nullable(UInt8)) ENGINE = Buffer(currentDatabase(), t_05894_buf, 1, 100000, 100000, 1000000, 1000000, 100000000, 100000000);
INSERT INTO t_05894_outer VALUES (4);
CREATE TABLE t_05894_same AS t_05894_local ENGINE = Buffer(currentDatabase(), t_05894_buf, 1, 100000, 100000, 1000000, 1000000, 100000000, 100000000);
CREATE TABLE t_05894_src (a UInt8) ENGINE = Null;
CREATE MATERIALIZED VIEW t_05894_mv TO t_05894_buf (a Nullable(UInt8)) AS SELECT a FROM t_05894_src;
CREATE MATERIALIZED VIEW t_05894_rmv REFRESH EVERY 1 YEAR APPEND TO t_05894_buf (a Nullable(UInt8)) EMPTY AS SELECT a FROM t_05894_src;

SELECT sum(a) FROM t_05894_outer SETTINGS prefer_localhost_replica = 0; -- { serverError NOT_IMPLEMENTED }
SELECT sum(a) FROM t_05894_outer SETTINGS prefer_localhost_replica = 1; -- { serverError NOT_IMPLEMENTED }
SELECT a FROM t_05894_outer ORDER BY a SETTINGS prefer_localhost_replica = 0; -- { serverError NOT_IMPLEMENTED }
SELECT a, count() FROM t_05894_outer GROUP BY a ORDER BY a SETTINGS prefer_localhost_replica = 0; -- { serverError NOT_IMPLEMENTED }
SELECT a FROM t_05894_mv ORDER BY a SETTINGS prefer_localhost_replica = 0; -- { serverError NOT_IMPLEMENTED }
SELECT a FROM t_05894_rmv ORDER BY a SETTINGS prefer_localhost_replica = 0; -- { serverError NOT_IMPLEMENTED }
SELECT a FROM t_05894_same ORDER BY a SETTINGS prefer_localhost_replica = 0;
SELECT sum(a) FROM t_05894_same SETTINGS prefer_localhost_replica = 0;

DROP TABLE t_05894_same;
DROP TABLE t_05894_outer;
DROP TABLE t_05894_rmv;
DROP TABLE t_05894_mv;
DROP TABLE t_05894_src;
DROP TABLE t_05894_buf;
DROP TABLE t_05894_dist;
DROP TABLE t_05894_local;
