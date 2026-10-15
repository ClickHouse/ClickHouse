-- Tags: distributed
-- A server that receives a query from another server skips positional arguments only in that query,
-- not in the queries it analyzes itself (materialized views, row policies, view(), parameterized views).
-- A query re-analyzed from its printed form (here by `Buffer`) skips them on every node, also inside a view.

SET enable_positional_arguments = 1;
SET prefer_localhost_replica = 0;
SET async_insert = 0;

DROP TABLE IF EXISTS t_src_dist;
DROP TABLE IF EXISTS mv_pos;
DROP TABLE IF EXISTS t_mv_dst;
DROP TABLE IF EXISTS t_src;
DROP TABLE IF EXISTS t_rp;
DROP TABLE IF EXISTS pv;
DROP TABLE IF EXISTS t_pos;
DROP TABLE IF EXISTS t_buf;
DROP TABLE IF EXISTS t_buf_dist;
DROP TABLE IF EXISTS t_buf_dst;

CREATE TABLE t_src (k Int64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_mv_dst (k Int64, c UInt64) ENGINE = MergeTree ORDER BY k;
CREATE MATERIALIZED VIEW mv_pos TO t_mv_dst AS SELECT k, count() AS c FROM t_src GROUP BY 1;

SELECT 'materialized view, insert into remote()';
INSERT INTO FUNCTION remote('127.0.0.2', currentDatabase(), t_src) SELECT number % 2 FROM numbers(10);
SELECT k, sum(c) FROM t_mv_dst GROUP BY k ORDER BY k;

SELECT 'materialized view, background insert into Distributed';
TRUNCATE TABLE t_src;
TRUNCATE TABLE t_mv_dst;
CREATE TABLE t_src_dist AS t_src ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t_src, k);
INSERT INTO t_src_dist SELECT number % 2 FROM numbers(10) SETTINGS distributed_foreground_insert = 0;
SYSTEM FLUSH DISTRIBUTED t_src_dist;
SELECT count() FROM t_src;
SELECT k, sum(c) FROM t_mv_dst GROUP BY k ORDER BY k;

SELECT 'view()';
SELECT * FROM remote('127.0.0.2', view(SELECT number % 3 AS k, count() FROM numbers(10) GROUP BY 1)) ORDER BY k;

SELECT 'row policy';
CREATE TABLE t_rp (k Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_rp SELECT number FROM numbers(5);
CREATE ROW POLICY rp_pos_05401 ON t_rp USING k IN (SELECT number FROM numbers(5) ORDER BY 1 DESC LIMIT 1) TO ALL;
SELECT * FROM remote('127.0.0.2', currentDatabase(), t_rp);
DROP ROW POLICY rp_pos_05401 ON t_rp;

SELECT 'parameterized view';
CREATE TABLE t_pos (k UInt8) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_pos SELECT number FROM numbers(5);
CREATE VIEW pv AS SELECT k, count() AS c FROM t_pos WHERE k < {n:UInt8} GROUP BY 1;
SELECT k FROM remote('127.0.0.2', currentDatabase(), t_pos) WHERE k IN (SELECT k FROM pv(n = 3)) ORDER BY k;

SELECT 'received query';
SELECT 5, k FROM remote('127.0.0.2', currentDatabase(), t_pos) ORDER BY ALL;

SELECT 'printed query re-analyzed in a view';
CREATE TABLE t_buf_dst (k Int64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_buf_dist AS t_buf_dst ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_buf_dst);
CREATE TABLE t_buf AS t_buf_dst ENGINE = Buffer(currentDatabase(), t_buf_dist, 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000);
INSERT INTO t_buf SELECT number FROM numbers(3);
SELECT * FROM view(SELECT 5, k FROM t_buf ORDER BY ALL) ORDER BY ALL;

DROP TABLE t_src_dist;
DROP TABLE mv_pos;
DROP TABLE t_mv_dst;
DROP TABLE t_src;
DROP TABLE t_rp;
DROP TABLE pv;
DROP TABLE t_pos;
DROP TABLE t_buf;
DROP TABLE t_buf_dist;
DROP TABLE t_buf_dst;
