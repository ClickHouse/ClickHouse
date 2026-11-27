-- A Buffer over Distributed read above FetchColumns through a table that declares other columns than the Buffer.

DROP TABLE IF EXISTS t_05894_same;
DROP TABLE IF EXISTS t_05894_outer;
DROP TABLE IF EXISTS t_05894_rmv;
DROP TABLE IF EXISTS t_05894_mv;
DROP TABLE IF EXISTS t_05894_src;
DROP TABLE IF EXISTS t_05894_buf;
DROP TABLE IF EXISTS t_05894_dist;
DROP TABLE IF EXISTS t_05894_local;
DROP TABLE IF EXISTS t_05894_outer5;
DROP TABLE IF EXISTS t_05894_outer3;
DROP TABLE IF EXISTS t_05894_outer2;
DROP TABLE IF EXISTS t_05894_mv2;
DROP TABLE IF EXISTS t_05894_src2;
DROP TABLE IF EXISTS t_05894_buf2;
DROP TABLE IF EXISTS t_05894_dist2;
DROP TABLE IF EXISTS t_05894_local2;
DROP TABLE IF EXISTS t_05894_mv3;
DROP TABLE IF EXISTS t_05894_src3;
DROP TABLE IF EXISTS t_05894_outer4;
DROP TABLE IF EXISTS t_05894_buf3;
DROP TABLE IF EXISTS t_05894_dist3;
DROP TABLE IF EXISTS t_05894_local3;
DROP TABLE IF EXISTS t_05894_outer7;
DROP TABLE IF EXISTS t_05894_outer6;
DROP TABLE IF EXISTS t_05894_buf5;
DROP TABLE IF EXISTS t_05894_buf4;
DROP TABLE IF EXISTS t_05894_dist4;
DROP TABLE IF EXISTS t_05894_local4;
DROP TABLE IF EXISTS t_05894_outer8;
DROP TABLE IF EXISTS t_05894_buf6;
DROP TABLE IF EXISTS t_05894_dist5;
DROP TABLE IF EXISTS t_05894_local5;

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
SELECT _table FROM t_05894_same ORDER BY _table SETTINGS prefer_localhost_replica = 0;
SELECT _table, count() FROM t_05894_outer GROUP BY _table ORDER BY _table SETTINGS prefer_localhost_replica = 1;
SELECT _table FROM t_05894_mv ORDER BY _table SETTINGS prefer_localhost_replica = 0;
SELECT count() FROM t_05894_outer SETTINGS prefer_localhost_replica = 0; -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM t_05894_mv SETTINGS prefer_localhost_replica = 1; -- { serverError NOT_IMPLEMENTED }
SELECT sum(1) FROM t_05894_rmv SETTINGS prefer_localhost_replica = 0; -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM t_05894_outer SETTINGS additional_table_filters = {'t_05894_outer': 'a IS NOT NULL'}, prefer_localhost_replica = 0; -- { serverError NOT_IMPLEMENTED }

DROP TABLE t_05894_same;
DROP TABLE t_05894_outer;
DROP TABLE t_05894_rmv;
DROP TABLE t_05894_mv;
DROP TABLE t_05894_src;
DROP TABLE t_05894_buf;
DROP TABLE t_05894_dist;
DROP TABLE t_05894_local;

CREATE TABLE t_05894_local2 (x UInt64, y UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_05894_local2 VALUES (1, 1), (2, 2);
CREATE TABLE t_05894_dist2 AS t_05894_local2 ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_05894_local2);
CREATE TABLE t_05894_buf2 AS t_05894_local2 ENGINE = Buffer(currentDatabase(), t_05894_dist2, 1, 100000, 100000, 1000000, 1000000, 100000000, 100000000);
INSERT INTO t_05894_buf2 VALUES (3, 3);
CREATE TABLE t_05894_outer2 (x UInt64) ENGINE = Buffer(currentDatabase(), t_05894_buf2, 1, 100000, 100000, 1000000, 1000000, 100000000, 100000000);
INSERT INTO t_05894_outer2 VALUES (4);
CREATE TABLE t_05894_outer3 (x UInt64, y String) ENGINE = Buffer(currentDatabase(), t_05894_buf2, 1, 100000, 100000, 1000000, 1000000, 100000000, 100000000);
CREATE TABLE t_05894_src2 (x UInt64) ENGINE = Null;
CREATE MATERIALIZED VIEW t_05894_mv2 TO t_05894_buf2 (x UInt64) AS SELECT x FROM t_05894_src2;
CREATE TABLE t_05894_outer5 (x UInt64, y UInt8 ALIAS x) ENGINE = Buffer(currentDatabase(), t_05894_buf2, 1, 100000, 100000, 1000000, 1000000, 100000000, 100000000);

SELECT count() FROM t_05894_outer2 SETTINGS prefer_localhost_replica = 0;
SELECT count() FROM t_05894_outer2 SETTINGS prefer_localhost_replica = 1;
SELECT count() FROM t_05894_mv2 SETTINGS prefer_localhost_replica = 0;
SELECT count() FROM t_05894_outer3 SETTINGS prefer_localhost_replica = 0;
SELECT x FROM t_05894_mv2 ORDER BY x SETTINGS prefer_localhost_replica = 0;
SELECT y FROM t_05894_outer3 ORDER BY y SETTINGS prefer_localhost_replica = 0; -- { serverError NOT_IMPLEMENTED }
SELECT count() FROM t_05894_outer5 SETTINGS additional_table_filters = {'t_05894_outer5': 'y > 1'}, prefer_localhost_replica = 0;

DROP TABLE t_05894_outer5;
DROP TABLE t_05894_outer3;
DROP TABLE t_05894_outer2;
DROP TABLE t_05894_mv2;
DROP TABLE t_05894_src2;
DROP TABLE t_05894_buf2;
DROP TABLE t_05894_dist2;
DROP TABLE t_05894_local2;

CREATE TABLE t_05894_local3 (x UInt64, y UInt8, z UInt8 ALIAS x > 1) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_05894_local3 (x, y) VALUES (1, 1);
CREATE TABLE t_05894_dist3 AS t_05894_local3 ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_05894_local3);
CREATE TABLE t_05894_buf3 AS t_05894_local3 ENGINE = Buffer(currentDatabase(), t_05894_dist3, 1, 100000, 100000, 1000000, 1000000, 100000000, 100000000);
INSERT INTO t_05894_buf3 (x, y) VALUES (2, 2);
CREATE TABLE t_05894_outer4 (x UInt64, y UInt8, z UInt8 ALIAS x > 2) ENGINE = Buffer(currentDatabase(), t_05894_buf3, 1, 100000, 100000, 1000000, 1000000, 100000000, 100000000);
INSERT INTO t_05894_outer4 (x, y) VALUES (3, 3);
CREATE TABLE t_05894_src3 (x UInt64) ENGINE = Null;
CREATE MATERIALIZED VIEW t_05894_mv3 TO t_05894_buf3 (x UInt64, z UInt8 ALIAS x > 1) AS SELECT x FROM t_05894_src3;

SELECT count() FROM t_05894_outer4 SETTINGS additional_table_filters = {'t_05894_outer4': 'z > 0'}, prefer_localhost_replica = 0;
SELECT y FROM t_05894_outer4 ORDER BY y SETTINGS additional_table_filters = {'t_05894_outer4': 'z > 0'}, prefer_localhost_replica = 1;
SELECT y FROM t_05894_outer4 ORDER BY y SETTINGS prefer_localhost_replica = 0;
SELECT count() FROM t_05894_mv3 SETTINGS additional_table_filters = {'t_05894_mv3': 'z > 0'}, prefer_localhost_replica = 0;

DROP TABLE t_05894_mv3;
DROP TABLE t_05894_src3;
DROP TABLE t_05894_outer4;
DROP TABLE t_05894_buf3;
DROP TABLE t_05894_dist3;
DROP TABLE t_05894_local3;

CREATE TABLE t_05894_local4 (x UInt64, z UInt8 ALIAS _table = 't_05894_buf4') ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_05894_dist4 AS t_05894_local4 ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_05894_local4);
CREATE TABLE t_05894_buf4 (x UInt64, z UInt8 ALIAS _table = 't_05894_outer6') ENGINE = Buffer(currentDatabase(), t_05894_dist4, 1, 100000, 100000, 1000000, 1000000, 100000000, 100000000);
INSERT INTO t_05894_buf4 VALUES (2);
CREATE TABLE t_05894_outer6 (x UInt64, z UInt8 ALIAS _table = 't_05894_buf4') ENGINE = Buffer(currentDatabase(), t_05894_buf4, 1, 100000, 100000, 1000000, 1000000, 100000000, 100000000);
CREATE TABLE t_05894_buf5 (x UInt64) ENGINE = Buffer(currentDatabase(), t_05894_dist4, 1, 100000, 100000, 1000000, 1000000, 100000000, 100000000);
INSERT INTO t_05894_buf5 VALUES (3);
CREATE TABLE t_05894_outer7 (x UInt64, z UInt8 ALIAS _table = 't_05894_buf5') ENGINE = Buffer(currentDatabase(), t_05894_buf5, 1, 100000, 100000, 1000000, 1000000, 100000000, 100000000);

SELECT _table FROM t_05894_outer6 SETTINGS additional_table_filters = {'t_05894_outer6': 'z > 0'}, prefer_localhost_replica = 0;
SELECT _table FROM t_05894_outer7 SETTINGS additional_table_filters = {'t_05894_outer7': 'z > 0'}, prefer_localhost_replica = 1;
SELECT x FROM t_05894_outer7 SETTINGS additional_table_filters = {'t_05894_outer7': 'z > 0'}, prefer_localhost_replica = 0;

DROP TABLE t_05894_outer7;
DROP TABLE t_05894_outer6;
DROP TABLE t_05894_buf5;
DROP TABLE t_05894_buf4;
DROP TABLE t_05894_dist4;
DROP TABLE t_05894_local4;

CREATE TABLE t_05894_local5 (t Tuple(a UInt64, b UInt8)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_05894_local5 VALUES ((1, 1));
CREATE TABLE t_05894_dist5 AS t_05894_local5 ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_05894_local5);
CREATE TABLE t_05894_buf6 AS t_05894_local5 ENGINE = Buffer(currentDatabase(), t_05894_dist5, 1, 100000, 100000, 1000000, 1000000, 100000000, 100000000);
INSERT INTO t_05894_buf6 VALUES ((2, 2));
CREATE TABLE t_05894_outer8 (`t.b` UInt8) ENGINE = Buffer(currentDatabase(), t_05894_buf6, 1, 100000, 100000, 1000000, 1000000, 100000000, 100000000);

SELECT `t.b` FROM t_05894_outer8 ORDER BY `t.b` SETTINGS prefer_localhost_replica = 0;
SELECT count() FROM t_05894_outer8 SETTINGS prefer_localhost_replica = 0;
SELECT 1 FROM t_05894_outer8 SETTINGS prefer_localhost_replica = 1;

DROP TABLE t_05894_outer8;
DROP TABLE t_05894_buf6;
DROP TABLE t_05894_dist5;
DROP TABLE t_05894_local5;
