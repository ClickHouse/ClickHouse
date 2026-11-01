-- Random settings limits: enable_join_runtime_filters=(1, 1); query_plan_optimize_join_order_randomize=(0, 0)
-- The JOIN-kind rewrites decide from a one-row evaluation of the WHERE over stand-in rows.
-- `isConstant` depends on the constness of its argument, so it must see the full column
-- that a row of the join result has at runtime.

SET query_plan_convert_outer_join_to_inner_join = 1;
SET query_plan_convert_any_join_to_semi_or_anti_join = 1;
SET query_plan_convert_outer_join_to_inner_join_transitively = 1;
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

SELECT 'left, constant below GROUP BY';
SELECT count() FROM numbers(3) AS a
LEFT JOIN (SELECT number, flag FROM (SELECT number, toNullable(1) AS flag FROM numbers(2)) GROUP BY number, flag) AS b ON a.number = b.number
WHERE isConstant(b.flag) = 1;
SELECT count() FROM (SELECT number, flag FROM (SELECT number, toNullable(1) AS flag FROM numbers(2)) GROUP BY number, flag) AS a
RIGHT JOIN numbers(3) AS b ON a.number = b.number
WHERE isConstant(a.flag) = 1;

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

-- The transitive rewrite decides from a condition further up the plan; without the padding of the converted side,
-- a function above reads that side's constants as constants.
SELECT 'transitive, WHERE above an enclosing join';
SELECT a.number, b.flag, c.number FROM numbers(3) AS a LEFT JOIN (SELECT number, toNullable(1) AS flag FROM numbers(2)) AS b ON a.number = b.number
INNER JOIN numbers(3) AS c ON b.flag = c.number WHERE isConstant(b.flag) = 0 ORDER BY 1;
SELECT count() FROM numbers(3) AS a LEFT JOIN (SELECT number, toNullable(1) AS flag FROM numbers(2)) AS b ON a.number = b.number
INNER JOIN numbers(3) AS c ON b.flag = c.number WHERE isConstant(b.flag) = 1;

SELECT 'transitive, enclosing ON';
SELECT a.number, b.flag, c.number FROM numbers(3) AS a LEFT JOIN (SELECT number, toNullable(1) AS flag FROM numbers(2)) AS b ON a.number = b.number
INNER JOIN numbers(3) AS c ON b.flag = c.number AND isConstant(b.flag) = 0 ORDER BY 1;
SELECT count() FROM numbers(3) AS a LEFT JOIN (SELECT number, toNullable(1) AS flag FROM numbers(2)) AS b ON a.number = b.number
INNER JOIN numbers(3) AS c ON b.flag = c.number AND isConstant(b.flag) = 1;

SELECT 'transitive, RIGHT JOIN projection';
SELECT a.number, isConstant(a.flag), c.number FROM (SELECT number, toNullable(1) AS flag FROM numbers(2)) AS a RIGHT JOIN numbers(3) AS b ON a.number = b.number
INNER JOIN numbers(3) AS c ON a.flag = c.number ORDER BY ALL;

SELECT 'transitive, nested LEFT JOIN side';
SELECT a.number, b.flag, c.number FROM numbers(3) AS a LEFT JOIN (SELECT x.number AS number, y.flag AS flag FROM numbers(2) AS x LEFT JOIN (SELECT number, toNullable(1) AS flag FROM numbers(2)) AS y ON x.number = y.number) AS b ON a.number = b.number
INNER JOIN numbers(3) AS c ON b.flag = c.number WHERE isConstant(b.flag) = 0 ORDER BY 1;
SELECT count() FROM numbers(3) AS a LEFT JOIN (SELECT x.number AS number, y.flag AS flag FROM numbers(2) AS x LEFT JOIN (SELECT number, toNullable(1) AS flag FROM numbers(2)) AS y ON x.number = y.number) AS b ON a.number = b.number
INNER JOIN numbers(3) AS c ON b.flag = c.number WHERE isConstant(b.flag) = 1;

SELECT 'transitive, constant below GROUP BY';
SELECT a.number, b.flag, c.number FROM numbers(3) AS a
LEFT JOIN (SELECT number, flag FROM (SELECT number, toNullable(1) AS flag FROM numbers(2)) GROUP BY number, flag) AS b ON a.number = b.number
INNER JOIN numbers(3) AS c ON b.flag = c.number WHERE isConstant(b.flag) = 0 ORDER BY 1;
SELECT count() FROM numbers(3) AS a
LEFT JOIN (SELECT number, flag FROM (SELECT number, toNullable(1) AS flag FROM numbers(2)) GROUP BY number, flag) AS b ON a.number = b.number
INNER JOIN numbers(3) AS c ON b.flag = c.number WHERE isConstant(b.flag) = 1;

SELECT 'transitive, read above a PASTE JOIN';
SELECT isConstant(t.x), t.n FROM (SELECT a.flag AS x, b.number AS n FROM (SELECT number, toNullable(1) AS flag FROM numbers(2)) AS a
RIGHT JOIN numbers(3) AS b ON a.number = b.number INNER JOIN numbers(3) AS c ON a.flag = c.number) AS t
PASTE JOIN (SELECT number FROM numbers(3)) AS u ORDER BY ALL SETTINGS query_plan_join_swap_table = 0;

SELECT 'transitive, join_use_nulls';
SELECT a.number, b.number, c.number FROM numbers(3) AS a LEFT JOIN (SELECT number, 1 AS flag FROM numbers(2)) AS b ON a.number = b.number
INNER JOIN numbers(3) AS c ON b.number = c.number AND isConstant(b.flag) = 0 ORDER BY 1 SETTINGS join_use_nulls = 1;

-- Controls: a representation read of a column that is not a constant does not block the transitive rewrite.
SELECT 'transitive, non-constant column';
SELECT a.number, b.n, c.number FROM numbers(3) AS a LEFT JOIN (SELECT number, toNullable(number) AS n FROM numbers(2)) AS b ON a.number = b.number
INNER JOIN numbers(3) AS c ON b.n = c.number WHERE isConstant(b.n) = 0 ORDER BY 1;
SELECT count() FROM (
    EXPLAIN actions = 1 SELECT a.number, b.n, c.number FROM numbers(3) AS a LEFT JOIN (SELECT number, toNullable(number) AS n FROM numbers(2)) AS b ON a.number = b.number
    INNER JOIN numbers(3) AS c ON b.n = c.number WHERE isConstant(b.n) = 0
) WHERE explain ILIKE '%Type: inner%';

SELECT 'transitive, joinGet';
SELECT a.number, b.n, c.number FROM numbers(3) AS a LEFT JOIN (SELECT number, toNullable(number + 1) AS n FROM numbers(2)) AS b ON a.number = b.number
INNER JOIN numbers(3) AS c ON b.n = c.number WHERE joinGet('t_join_get', 'attr', b.number) != 'z' ORDER BY 1;
SELECT count() FROM (
    EXPLAIN actions = 1 SELECT a.number, b.n, c.number FROM numbers(3) AS a LEFT JOIN (SELECT number, toNullable(number + 1) AS n FROM numbers(2)) AS b ON a.number = b.number
    INNER JOIN numbers(3) AS c ON b.n = c.number WHERE joinGet('t_join_get', 'attr', b.number) != 'z'
) WHERE explain ILIKE '%Type: inner%';

DROP TABLE t_join_get;
