-- The JOIN-kind rewrites decide from a one-row evaluation of the WHERE over stand-in rows.
-- `isConstant` depends on the constness of its argument, so it must see the full column
-- that a row of the join result has at runtime.

SET query_plan_convert_outer_join_to_inner_join = 1;
SET query_plan_convert_any_join_to_semi_or_anti_join = 1;
SET enable_parallel_replicas = 0;
SET join_use_nulls = 0;

SELECT 'left';
SELECT a.number, b.number FROM numbers(3) AS a LEFT JOIN (SELECT number FROM numbers(2)) AS b ON a.number = b.number
WHERE isConstant(b.number) = 0 ORDER BY 1;

SELECT 'right';
SELECT a.number, b.number FROM (SELECT number FROM numbers(2)) AS a RIGHT JOIN numbers(3) AS b ON a.number = b.number
WHERE isConstant(a.number) = 0 ORDER BY 2;

SELECT 'full';
SELECT a.number, b.number FROM (SELECT number FROM numbers(2)) AS a FULL JOIN (SELECT number + 1 AS number FROM numbers(2)) AS b ON a.number = b.number
WHERE isConstant(b.number) = 0 ORDER BY 1, 2;

SELECT 'any left';
SELECT a.number, b.number FROM numbers(3) AS a ANY LEFT JOIN (SELECT number FROM numbers(2)) AS b ON a.number = b.number
WHERE isConstant(b.number) = 0 ORDER BY 1;

SELECT 'join_use_nulls';
SELECT a.number, b.number FROM numbers(3) AS a LEFT JOIN (SELECT number FROM numbers(2)) AS b ON a.number = b.number
WHERE isConstant(b.number) = 0 ORDER BY 1 SETTINGS join_use_nulls = 1;

SELECT 'lambda';
SELECT a.number, b.number FROM numbers(3) AS a LEFT JOIN (SELECT number FROM numbers(2)) AS b ON a.number = b.number
WHERE arrayExists(y -> isConstant(b.number) = 0, [1]) ORDER BY 1;

-- Matched rows: a constant of the right subquery is a full column once it has been through the join.
SELECT 'any left, matched rows';
SELECT a.number, b.flag FROM numbers(3) AS a ANY LEFT JOIN (SELECT number + 1 AS number, 1 AS flag FROM numbers(2)) AS b ON a.number = b.number
WHERE isConstant(b.flag) = 0 OR b.flag = 0 ORDER BY 1;

SELECT 'left, constant column';
SELECT a.number, b.flag FROM numbers(3) AS a LEFT JOIN (SELECT number, 1 AS flag FROM numbers(2)) AS b ON a.number = b.number
WHERE isConstant(b.flag) = 0 ORDER BY 1;

-- A constant of the null-extended side stays a constant where the rewritten join lets the filter run.
SELECT 'left, constant column and value';
SELECT a.number, b.flag FROM numbers(3) AS a LEFT JOIN (SELECT number, 1 AS flag FROM numbers(2)) AS b ON a.number = b.number
WHERE isConstant(b.flag) = 0 AND b.flag = 1 ORDER BY 1;

SELECT 'left, Enum constant';
SELECT a.number, b.flag FROM numbers(3) AS a LEFT JOIN (SELECT number, 1 AS flag, CAST('x', 'Enum8(\'x\' = 1)') AS e FROM numbers(2)) AS b ON a.number = b.number
WHERE isConstant(b.e) = 0 AND b.flag = 1 ORDER BY 1;

-- Controls: the rewrite still happens for a `joinGet` filter, also inside an OR of conjunctions, and for an OR of
-- conjunctions with conditions on the other side.
DROP TABLE IF EXISTS t_join_get;
CREATE TABLE t_join_get (id UInt64, attr String) ENGINE = Join(ANY, LEFT, id);
INSERT INTO t_join_get VALUES (1, 'x'), (2, 'y');

SELECT 'joinGet';
SELECT a.number, b.number FROM numbers(3) AS a LEFT JOIN (SELECT number + 1 AS number FROM numbers(2)) AS b ON a.number = b.number
WHERE joinGet('t_join_get', 'attr', b.number) = 'x' ORDER BY 1;
SELECT count() FROM (
    EXPLAIN actions = 1 SELECT a.number, b.number FROM numbers(3) AS a LEFT JOIN (SELECT number + 1 AS number FROM numbers(2)) AS b ON a.number = b.number
    WHERE joinGet('t_join_get', 'attr', b.number) = 'x'
) WHERE explain ILIKE '%Type: inner%';

SELECT 'joinGet in an OR of conjunctions';
SELECT a.number, b.number FROM numbers(3) AS a LEFT JOIN (SELECT number + 1 AS number FROM numbers(2)) AS b ON a.number = b.number
WHERE (joinGet('t_join_get', 'attr', b.number) = 'x' AND a.number < 2) OR (b.number = 5 AND a.number > 7) ORDER BY 1;
SELECT count() FROM (
    EXPLAIN actions = 1 SELECT a.number, b.number FROM numbers(3) AS a LEFT JOIN (SELECT number + 1 AS number FROM numbers(2)) AS b ON a.number = b.number
    WHERE (joinGet('t_join_get', 'attr', b.number) = 'x' AND a.number < 2) OR (b.number = 5 AND a.number > 7)
) WHERE explain ILIKE '%Type: inner%';

SELECT 'or of conjunctions';
SELECT count() FROM (
    EXPLAIN actions = 1 SELECT a.number, b.number FROM numbers(3) AS a LEFT JOIN (SELECT number FROM numbers(2)) AS b ON a.number = b.number
    WHERE (b.number = 1 AND a.number < 2) OR (b.number = 5 AND a.number > 7)
) WHERE explain ILIKE '%Type: inner%';

DROP TABLE t_join_get;
