-- An equality with a constant is not a join key: `t5.xa = 1 OR t6.yb = 5` has no equality between the two tables,
-- so an ALL INNER join keeps it as one condition and `t5.xk = t6.yk` stays a join key that join reordering can use
-- (the same shape as issue #122510 with `=` instead of `>` in the branches of the OR).

-- The join plan depends on these settings, which the test runner randomizes.
SET enable_parallel_replicas = 0;
SET allow_general_join_planning = 1;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = false;

DROP TABLE IF EXISTS t5;
DROP TABLE IF EXISTS t6;
DROP TABLE IF EXISTS t7;

CREATE TABLE t5 (xk UInt64, xa UInt64) ENGINE = MergeTree ORDER BY xk;
CREATE TABLE t6 (yk UInt64, yb UInt64, yz UInt64) ENGINE = MergeTree ORDER BY yk;
CREATE TABLE t7 (zk UInt64, zc UInt64) ENGINE = MergeTree ORDER BY zk;

INSERT INTO t5 SELECT number, number % 7 FROM numbers(20000);
INSERT INTO t6 SELECT number, number % 11, number % 3 FROM numbers(20000);
INSERT INTO t7 SELECT number, number FROM numbers(3);

-- In an inner join, the equality next to an OR of equalities with constants on one table each stays a join key.
SELECT count() FROM (EXPLAIN SELECT count() FROM t5 JOIN t6 ON t5.xk = t6.yk AND (t5.xa = 1 OR t6.yb = 5)) WHERE match(explain, 'Join conditions: .*(xk = yk|yk = xk)') AND explain NOT LIKE '% OR %';
SELECT count() FROM (EXPLAIN SELECT count() FROM t5 JOIN t6 ON t5.xk = t6.yk AND (t5.xa = 1 OR t6.yb = 5) JOIN t7 ON t6.yz = t7.zk AND t5.xa = t7.zc) WHERE match(explain, 'Join conditions: .*(xk = yk|yk = xk)') AND explain NOT LIKE '% OR %';
SELECT count() FROM t5 JOIN t6 ON t5.xk = t6.yk AND (t5.xa = 1 OR t6.yb = 5);
SELECT count() FROM t5 JOIN t6 ON t5.xk = t6.yk AND (t5.xa = 1 OR t6.yb = 5) JOIN t7 ON t6.yz = t7.zk AND t5.xa = t7.zc;

-- An OR with an equality between the two tables in a branch is still split into clauses with that key.
SELECT count() FROM (EXPLAIN SELECT count() FROM t5 JOIN t6 ON t5.xk = t6.yk AND (t5.xa = t6.yb OR t6.yb = 5)) WHERE explain LIKE '%Join conditions: %xa = yb%' OR explain LIKE '%Join conditions: %yb = xa%';
SELECT count() FROM t5 JOIN t6 ON t5.xk = t6.yk AND (t5.xa = t6.yb OR t6.yb = 5);

-- In an outer join, an OR of equalities with constants on one table each keeps working.
SELECT count(), sum(t6.yb) FROM t5 LEFT JOIN t6 ON t5.xk = t6.yk AND (t5.xa = 1 OR t6.yb = 5);

DROP TABLE t5;
DROP TABLE t6;
DROP TABLE t7;
