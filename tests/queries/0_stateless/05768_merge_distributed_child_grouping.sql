-- grouping() over a Merge whose children include a Distributed table, so the local children are planned from the printed query.

DROP TABLE IF EXISTS t_05768_merge_dist;
DROP TABLE IF EXISTS t_05768_dist_a;
DROP TABLE IF EXISTS t_05768_a;
DROP TABLE IF EXISTS t_05768_b;

CREATE TABLE t_05768_a (id UInt64, a UInt8, b UInt8) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_05768_b AS t_05768_a;
INSERT INTO t_05768_a VALUES (1, 5, 5), (2, 5, 7);
INSERT INTO t_05768_b VALUES (3, 6, 6);
CREATE TABLE t_05768_dist_a AS t_05768_a ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_05768_a);
CREATE TABLE t_05768_merge_dist AS t_05768_a ENGINE = Merge(currentDatabase(), '^t_05768_dist_a$');

SELECT 'GROUPING SETS';
SELECT a, b, grouping(a, b), count() FROM merge(currentDatabase(), '^t_05768_(dist_a|b)$') GROUP BY GROUPING SETS ((a, b), (a), ()) ORDER BY ALL;
SELECT 'ROLLUP';
SELECT a, b, grouping(a, b), count() FROM merge(currentDatabase(), '^t_05768_(dist_a|b)$') GROUP BY ROLLUP(a, b) ORDER BY ALL;
SELECT 'CUBE';
SELECT a, b, grouping(a, b), count() FROM merge(currentDatabase(), '^t_05768_(dist_a|b)$') GROUP BY CUBE(a, b) HAVING grouping(a, b) != 1 ORDER BY ALL;

-- A nested Merge child is planned from the printed query: alone (Complete) and next to a local child (WithMergeableState).
SELECT 'nested Merge';
SELECT a, b, grouping(a, b), count() FROM merge(currentDatabase(), '^t_05768_merge_dist$') GROUP BY GROUPING SETS ((a, b), (a), ()) ORDER BY ALL;
SELECT 'nested Merge and local';
SELECT a, b, grouping(a, b), count() FROM merge(currentDatabase(), '^t_05768_(merge_dist|b)$') GROUP BY GROUPING SETS ((a, b), (a), ()) ORDER BY ALL;

DROP TABLE t_05768_merge_dist;
DROP TABLE t_05768_dist_a;
DROP TABLE t_05768_a;
DROP TABLE t_05768_b;
