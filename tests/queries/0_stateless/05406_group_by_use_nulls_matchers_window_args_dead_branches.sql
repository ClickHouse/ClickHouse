-- With `group_by_use_nulls` the matchers of the projection are expanded before the other clauses.
-- The results must be the same as without the setting.

-- The guard of a named window definition must not keep the nested function arguments after leaving it,
-- so a following `REPLACE` still rewrites the `WINDOW` clause for the following window functions.
SELECT t.c, count() OVER w AS before, max(* REPLACE (intDiv(c, 2) AS c)) AS m, count() OVER w AS after
FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY GROUPING SETS ((t.c))
WINDOW w AS (PARTITION BY identity(c))
ORDER BY t.c
SETTINGS group_by_use_nulls = 0;

SELECT t.c, count() OVER w AS before, max(* REPLACE (intDiv(c, 2) AS c)) AS m, count() OVER w AS after
FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY GROUPING SETS ((t.c))
WINDOW w AS (PARTITION BY identity(c))
ORDER BY t.c
SETTINGS group_by_use_nulls = 1;

-- An invalid matcher expression in an unreachable branch of `if` and `multiIf` is accepted.
SELECT if(0, identity(* REPLACE (missing AS c)), 7) FROM (SELECT number AS c FROM numbers(3)) AS t GROUP BY GROUPING SETS ((t.c)) SETTINGS group_by_use_nulls = 0;
SELECT if(0, identity(* REPLACE (missing AS c)), 7) FROM (SELECT number AS c FROM numbers(3)) AS t GROUP BY GROUPING SETS ((t.c)) SETTINGS group_by_use_nulls = 1;
SELECT if(0, identity(* APPLY noSuchFunction), 7) FROM (SELECT number AS c FROM numbers(3)) AS t GROUP BY GROUPING SETS ((t.c)) SETTINGS group_by_use_nulls = 1;
SELECT multiIf(1, 7, identity(* APPLY noSuchFunction)) FROM (SELECT number AS c FROM numbers(3)) AS t GROUP BY GROUPING SETS ((t.c)) SETTINGS group_by_use_nulls = 1;
SELECT count() OVER (PARTITION BY if(0, identity(* APPLY noSuchFunction), 7)) FROM (SELECT number AS c FROM numbers(3)) AS t GROUP BY GROUPING SETS ((t.c)) SETTINGS group_by_use_nulls = 1;

-- It is still reported when the branch is reachable.
SELECT if(t.c > 0, identity(* APPLY noSuchFunction), 7) FROM (SELECT number AS c FROM numbers(3)) AS t GROUP BY GROUPING SETS ((t.c)) SETTINGS group_by_use_nulls = 1; -- { serverError UNKNOWN_FUNCTION }

-- A valid matcher in a branch still rewrites the other clauses before they are resolved.
SELECT if(1, max(* REPLACE (c + 10 AS c)), 0) AS m FROM (SELECT number AS c FROM numbers(3)) AS t GROUP BY GROUPING SETS ((t.c)) HAVING c > 10 ORDER BY ALL SETTINGS group_by_use_nulls = 0;
SELECT if(1, max(* REPLACE (c + 10 AS c)), 0) AS m FROM (SELECT number AS c FROM numbers(3)) AS t GROUP BY GROUPING SETS ((t.c)) HAVING c > 10 ORDER BY ALL SETTINGS group_by_use_nulls = 1;
