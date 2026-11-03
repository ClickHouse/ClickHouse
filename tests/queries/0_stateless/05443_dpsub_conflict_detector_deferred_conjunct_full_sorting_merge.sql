-- An inner join condition reading the padded side of a nested `LEFT JOIN` (`t_c.z = t_d.z`) waits until
-- that `LEFT JOIN` has been applied, and no longer: waiting for all of its tables would place the condition
-- above them, with both of its columns on one input, as a one-sided condition of an inner join, which
-- `full_sorting_merge` cannot execute. Every query must return the same rows as without reordering.
-- https://github.com/ClickHouse/ClickHouse/issues/127417

SET enable_analyzer = 1;
SET join_use_nulls = 0;
SET query_plan_optimize_join_order_algorithm = 'dpsub,greedy';
SET query_plan_optimize_join_order_conflict_detector = 'c';
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_limit = 10;
SET use_hash_table_stats_for_join_reordering = 0;

DROP TABLE IF EXISTS t_a;
DROP TABLE IF EXISTS t_b;
DROP TABLE IF EXISTS t_c;
DROP TABLE IF EXISTS t_d;

CREATE TABLE t_a (k UInt32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_b (k UInt32, x UInt32, y UInt32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_c (x UInt32, z UInt32) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_d (y UInt32, z UInt32) ENGINE = MergeTree ORDER BY y;

INSERT INTO t_a SELECT number FROM numbers(10);
INSERT INTO t_b VALUES (1, 1, 1), (2, 2, 2), (3, 3, 3);
INSERT INTO t_c VALUES (1, 10), (2, 20);
INSERT INTO t_d VALUES (1, 10), (2, 0), (3, 0);

SELECT '-- full_sorting_merge';
SELECT t_b.k, t_c.z, t_d.z
FROM t_a
INNER JOIN t_b ON t_a.k = t_b.k
LEFT JOIN t_c ON t_b.x = t_c.x
INNER JOIN t_d ON t_b.y = t_d.y AND t_c.z = t_d.z
ORDER BY ALL
SETTINGS join_algorithm = 'full_sorting_merge';

SELECT '-- parallel_full_sorting_merge';
SELECT t_b.k, t_c.z, t_d.z
FROM t_a
INNER JOIN t_b ON t_a.k = t_b.k
LEFT JOIN t_c ON t_b.x = t_c.x
INNER JOIN t_d ON t_b.y = t_d.y AND t_c.z = t_d.z
ORDER BY ALL
SETTINGS join_algorithm = 'parallel_full_sorting_merge';

SELECT '-- hash';
SELECT t_b.k, t_c.z, t_d.z
FROM t_a
INNER JOIN t_b ON t_a.k = t_b.k
LEFT JOIN t_c ON t_b.x = t_c.x
INNER JOIN t_d ON t_b.y = t_d.y AND t_c.z = t_d.z
ORDER BY ALL
SETTINGS join_algorithm = 'hash';

DROP TABLE t_a;
DROP TABLE t_b;
DROP TABLE t_c;
DROP TABLE t_d;
