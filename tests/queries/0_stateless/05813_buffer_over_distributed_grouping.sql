-- grouping() over a Buffer whose destination is Distributed, so the buffers are planned from the printed query.

DROP TABLE IF EXISTS t_05813_buf;
DROP TABLE IF EXISTS t_05813_dist;
DROP TABLE IF EXISTS t_05813_local;

CREATE TABLE t_05813_local (a UInt8, b UInt8) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_05813_local VALUES (5, 5), (5, 7);
CREATE TABLE t_05813_dist AS t_05813_local ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_05813_local);
CREATE TABLE t_05813_buf AS t_05813_local ENGINE = Buffer(currentDatabase(), t_05813_dist, 1, 100000, 100000, 1000000, 1000000, 100000000, 100000000);
INSERT INTO t_05813_buf VALUES (6, 6);

SELECT 'GROUPING SETS';
SELECT a, b, grouping(a, b), count() FROM t_05813_buf GROUP BY GROUPING SETS ((a, b), (a), ()) ORDER BY ALL;
SELECT 'ROLLUP';
SELECT a, b, grouping(a, b), count() FROM t_05813_buf GROUP BY ROLLUP(a, b) ORDER BY ALL;
SELECT 'CUBE';
SELECT a, grouping(a), count() FROM t_05813_buf GROUP BY CUBE(a) ORDER BY ALL;

DROP TABLE t_05813_buf;
DROP TABLE t_05813_dist;
DROP TABLE t_05813_local;
