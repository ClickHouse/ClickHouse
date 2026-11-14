-- The condition of `if`/`multiIf` is not known when the matchers of the projection are expanded in advance
-- under `group_by_use_nulls`: an invalid `REPLACE` in an unreachable branch must not rewrite the other clauses.

SET enable_analyzer = 1;
SET group_by_use_nulls = 1;

SELECT 'condition by alias';
WITH 1 AS cond
SELECT if(cond, 7, max(* REPLACE (missing AS c)))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
HAVING c > 0
ORDER BY ALL;

WITH 1 AS cond
SELECT multiIf(cond, 7, max(* REPLACE (missing AS c)))
FROM (SELECT number AS c FROM numbers(3)) AS t
WHERE c > 0
GROUP BY GROUPING SETS ((t.c))
ORDER BY ALL;

SELECT 'condition constant after GROUP BY';
SELECT if(isNullable(t.c), 7, max(* REPLACE (missing AS c)))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
HAVING c > 0
ORDER BY ALL;

SELECT 'reachable branch with a non-constant condition';
SELECT if(t.c > 0, max(* REPLACE (c + 10 AS c)), 0) AS r
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c), ())
HAVING c > 10
ORDER BY ALL;

SELECT if(t.c > 0, max(* REPLACE (missing AS c)), 0)
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c)); -- { serverError UNKNOWN_IDENTIFIER }

-- An alias of a projection item without matchers resolves its named window as the projection item does,
-- even if a matcher of another named window rewrites the definition.
SELECT 'alias of a projection item without matchers';
SELECT t.c, count() OVER w AS cnt
FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY GROUPING SETS ((t.c))
WINDOW w2 AS (PARTITION BY * REPLACE (intDiv(c, 2) AS c)), w AS (PARTITION BY c)
QUALIFY cnt = 1
ORDER BY t.c;

SELECT t.c, count() OVER w AS cnt
FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY GROUPING SETS ((t.c))
WINDOW w2 AS (PARTITION BY * REPLACE (intDiv(c, 2) AS c)), w AS (PARTITION BY c)
QUALIFY cnt = 2
ORDER BY t.c;
