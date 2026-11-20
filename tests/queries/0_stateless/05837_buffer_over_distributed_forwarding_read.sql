-- A Buffer over Distributed read through a materialized view, an Alias or another Buffer, so the buffers are planned from the printed query.

DROP TABLE IF EXISTS t_05837_outer;
DROP TABLE IF EXISTS t_05837_alias;
DROP TABLE IF EXISTS t_05837_mv;
DROP TABLE IF EXISTS t_05837_src;
DROP TABLE IF EXISTS t_05837_buf;
DROP TABLE IF EXISTS t_05837_dist;
DROP TABLE IF EXISTS t_05837_local;

CREATE TABLE t_05837_local (a UInt8) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_05837_local VALUES (1), (2);
CREATE TABLE t_05837_dist AS t_05837_local ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_05837_local);
CREATE TABLE t_05837_buf AS t_05837_local ENGINE = Buffer(currentDatabase(), t_05837_dist, 1, 100000, 100000, 1000000, 1000000, 100000000, 100000000);
INSERT INTO t_05837_buf VALUES (3);
CREATE TABLE t_05837_src (a UInt8) ENGINE = Null;
CREATE MATERIALIZED VIEW t_05837_mv TO t_05837_buf AS SELECT a FROM t_05837_src;
CREATE TABLE t_05837_alias ENGINE = Alias(t_05837_buf);
CREATE TABLE t_05837_outer AS t_05837_local ENGINE = Buffer(currentDatabase(), t_05837_buf, 1, 100000, 100000, 1000000, 1000000, 100000000, 100000000);
INSERT INTO t_05837_outer VALUES (4);

SELECT 'buffer';
SELECT a FROM t_05837_buf ORDER BY a;
SELECT 'materialized view';
SELECT a FROM t_05837_mv ORDER BY a;
SELECT m.a, count() FROM t_05837_mv AS m GROUP BY m.a ORDER BY m.a;
SELECT 'alias';
SELECT a FROM t_05837_alias ORDER BY a;
SELECT 'buffer over buffer';
SELECT a FROM t_05837_outer ORDER BY a;
SELECT sum(a) FROM t_05837_outer;

DROP TABLE t_05837_outer;
DROP TABLE t_05837_alias;
DROP TABLE t_05837_mv;
DROP TABLE t_05837_src;
DROP TABLE t_05837_buf;
DROP TABLE t_05837_dist;
DROP TABLE t_05837_local;
