-- A filter above a subquery with a computed column reads only the columns of its condition. The other columns of the
-- read pass the filter and keep their distinct counts, so the join order uses the distinct count of the join key `k`.
-- With it, the join of the first two tables has an estimate of 100 million rows, against 1 billion rows for the join
-- of the last two tables, and it is the first join, as in the query without the filter.

DROP TABLE IF EXISTS t_passed_key_left;
DROP TABLE IF EXISTS t_passed_key_right;
DROP TABLE IF EXISTS t_passed_key_third;

CREATE TABLE t_passed_key_left (k UInt32, x UInt64, y UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_passed_key_right (k UInt32, j UInt32, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_passed_key_third (j UInt32, w UInt64) ENGINE = MergeTree ORDER BY j SETTINGS auto_statistics_types = '';
INSERT INTO t_passed_key_left SELECT number % 100, number, number FROM numbers(1000);
INSERT INTO t_passed_key_right SELECT number % 10, number % 10, number FROM numbers(100);
INSERT INTO t_passed_key_third SELECT number % 10, number FROM numbers(100);

SET explain_query_plan_default = 'legacy';
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_join_swap_table = 'auto';
SET enable_join_runtime_filters = 0;
-- The condition stays in a filter step above the read.
SET optimize_move_to_prewhere = 0;
-- The tables stand in for large ones.
SET param__internal_join_table_stat_hints = '{"t_passed_key_left": {"cardinality": 1000000, "distinct_keys": {"k": 1000}}, "t_passed_key_right": {"cardinality": 100000, "distinct_keys": {"k": 10, "j": 100}}, "t_passed_key_third": {"cardinality": 1000000, "distinct_keys": {"j": 100}}}';

SELECT '-- with the filter: the first join is the one by k';
EXPLAIN SELECT max(l.z), count() FROM (SELECT k, y * 2 AS z, x FROM t_passed_key_left) AS l
JOIN t_passed_key_right AS r ON l.k = r.k JOIN t_passed_key_third AS c ON r.j = c.j WHERE l.x > 5;
SELECT '-- without the filter: the same join order';
EXPLAIN SELECT max(l.z), count() FROM (SELECT k, y * 2 AS z, x FROM t_passed_key_left) AS l
JOIN t_passed_key_right AS r ON l.k = r.k JOIN t_passed_key_third AS c ON r.j = c.j;

DROP TABLE t_passed_key_left;
DROP TABLE t_passed_key_right;
DROP TABLE t_passed_key_third;
