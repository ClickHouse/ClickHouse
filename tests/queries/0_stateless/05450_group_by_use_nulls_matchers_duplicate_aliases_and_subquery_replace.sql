-- Identical definitions of a duplicated alias that refers to a named window rewritten by a matcher
-- of another named window are still identical with `group_by_use_nulls`.
SELECT count() OVER w AS cnt, count() OVER w AS cnt
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
WINDOW w2 AS (PARTITION BY * REPLACE (intDiv(c, 2) AS c)),
       w AS (PARTITION BY c)
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

WITH count() OVER w AS cnt
SELECT cnt, count() OVER w AS cnt
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
WINDOW w2 AS (PARTITION BY * REPLACE (intDiv(c, 2) AS c)),
       w AS (PARTITION BY c)
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

-- Conflicting definitions are still rejected.
SELECT count() OVER w AS cnt, count() OVER w2 AS cnt
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
WINDOW w2 AS (PARTITION BY * REPLACE (intDiv(c, 2) AS c)),
       w AS (PARTITION BY c)
ORDER BY ALL
SETTINGS group_by_use_nulls = 1; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }

-- A `REPLACE` transformer in a subquery does not rewrite `GROUP BY` of the outer query,
-- so the outer `APPLY` is still resolved against the Nullable keys.
SELECT t.c, * APPLY (x -> getSubcolumn(x, 'null')),
       (SELECT * REPLACE (1 AS c) FROM (SELECT 0 AS c))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY c WITH ROLLUP
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;
