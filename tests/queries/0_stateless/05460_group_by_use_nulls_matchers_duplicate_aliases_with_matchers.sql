-- The copies of the duplicated aliases that contain a matcher take the named window snapshots of their projection items,
-- so the definitions equivalent after the expansion stay equal, in either order.
SET enable_analyzer = 1;

SELECT count(t.c) OVER w AS cnt, count(t.*) OVER w AS cnt
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
WINDOW w2 AS (PARTITION BY * REPLACE (intDiv(c, 2) AS c)), w AS (PARTITION BY c)
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT count(t.*) OVER w AS cnt, count(t.c) OVER w AS cnt
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
WINDOW w2 AS (PARTITION BY * REPLACE (intDiv(c, 2) AS c)), w AS (PARTITION BY c)
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

-- Conflicting definitions are still an error.
SELECT count(t.c) OVER w AS cnt, sum(t.*) OVER w AS cnt
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
WINDOW w2 AS (PARTITION BY * REPLACE (intDiv(c, 2) AS c)), w AS (PARTITION BY c)
ORDER BY ALL
SETTINGS group_by_use_nulls = 1; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }

-- Many identical aliased items with a named window.
SELECT count() FROM
(
    SELECT max(number) AS c, max(number) AS c, max(number) AS c, max(number) AS c, count() OVER w AS cnt
    FROM numbers(3)
    GROUP BY number WITH ROLLUP
    WINDOW w AS (PARTITION BY number)
)
SETTINGS group_by_use_nulls = 1;
