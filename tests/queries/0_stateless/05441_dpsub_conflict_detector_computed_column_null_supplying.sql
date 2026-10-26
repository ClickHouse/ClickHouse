-- A subquery joining two tables on the null-supplying side of an outer join may be flattened into the
-- outer join's group of tables, but a column it computes must keep its value for the padded rows:
-- `bx + 1 AS v` is the default `0` (or `NULL`) for a row of `t_a` without a match, not `bx + 1` of the
-- padded `bx`, which would be `1`. A subquery that only selects or renames columns is still flattened.
-- Every query must return the same rows as without reordering.

SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_algorithm = 'dpsub,greedy';
SET query_plan_optimize_join_order_conflict_detector = 'c';
SET use_hash_table_stats_for_join_reordering = 0;

DROP TABLE IF EXISTS t_a;
DROP TABLE IF EXISTS t_b;
DROP TABLE IF EXISTS t_c;
DROP TABLE IF EXISTS t_d;
DROP TABLE IF EXISTS t_na;
DROP TABLE IF EXISTS t_nb;
DROP TABLE IF EXISTS t_nc;

CREATE TABLE t_a (ak UInt32) ENGINE = Memory;
CREATE TABLE t_b (bk UInt32, bx UInt32) ENGINE = Memory;
CREATE TABLE t_c (ck UInt32) ENGINE = Memory;
CREATE TABLE t_d (dk UInt32) ENGINE = Memory;
INSERT INTO t_a VALUES (1), (2);
INSERT INTO t_b VALUES (1, 10);
INSERT INTO t_c VALUES (1);
INSERT INTO t_d VALUES (1), (0);

CREATE TABLE t_na (ak Nullable(UInt32)) ENGINE = Memory;
CREATE TABLE t_nb (bk Nullable(UInt32), bx Nullable(UInt32)) ENGINE = Memory;
CREATE TABLE t_nc (ck Nullable(UInt32)) ENGINE = Memory;
INSERT INTO t_na VALUES (1), (2);
INSERT INTO t_nb VALUES (1, 10);
INSERT INTO t_nc VALUES (1);

SELECT '-- LEFT JOIN';
SELECT ak, s.bk, s.v
FROM t_a LEFT JOIN (SELECT bk, bx + 1 AS v FROM t_b JOIN t_c ON bk = ck) AS s ON ak = s.bk
ORDER BY ALL;

SELECT '-- RIGHT JOIN';
SELECT ak, s.bk, s.v
FROM (SELECT bk, bx + 1 AS v FROM t_b JOIN t_c ON bk = ck) AS s RIGHT JOIN t_a ON ak = s.bk
ORDER BY ALL;

SELECT '-- FULL JOIN';
SELECT ak, s.bk, s.v
FROM t_a FULL JOIN (SELECT bk, bx + 1 AS v FROM t_b JOIN t_c ON bk = ck) AS s ON ak = s.bk
ORDER BY ALL;

-- The padded row has `v = 0`, which matches `t_d.dk = 0`.
SELECT '-- computed column used by a later join';
SELECT ak, s.v, t_d.dk
FROM t_a LEFT JOIN (SELECT bk, bx + 1 AS v FROM t_b JOIN t_c ON bk = ck) AS s ON ak = s.bk
JOIN t_d ON s.v = t_d.dk
ORDER BY ALL;

SELECT '-- constant column';
SELECT ak, s.bk, s.one
FROM t_a LEFT JOIN (SELECT bk, 1 AS one FROM t_b JOIN t_c ON bk = ck) AS s ON ak = s.bk
ORDER BY ALL;

SELECT '-- renamed column';
SELECT ak, s.x
FROM t_a LEFT JOIN (SELECT bk, bx AS x FROM t_b JOIN t_c ON bk = ck) AS s ON ak = s.bk
ORDER BY ALL;

SELECT '-- LEFT JOIN, join_use_nulls = 1';
SELECT ak, s.bk, s.v
FROM t_na LEFT JOIN (SELECT bk, toNullable(ifNull(bx, 0) + 1) AS v FROM t_nb JOIN t_nc ON bk = ck) AS s ON ak = s.bk
ORDER BY ALL
SETTINGS join_use_nulls = 1;

DROP TABLE t_a;
DROP TABLE t_b;
DROP TABLE t_c;
DROP TABLE t_d;
DROP TABLE t_na;
DROP TABLE t_nb;
DROP TABLE t_nc;
