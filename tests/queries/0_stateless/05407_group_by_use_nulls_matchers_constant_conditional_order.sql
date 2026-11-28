-- With constant conditions, the unreachable branches of `if` and `multiIf` are resolved before the selected one,
-- so a `REPLACE` transformer in an unreachable branch rewrites the selected branch regardless of `group_by_use_nulls`.

SELECT 'if, true';
SELECT if(1, max(c), max(* REPLACE (0 AS c)))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
ORDER BY ALL
SETTINGS group_by_use_nulls = 0;
SELECT if(1, max(c), max(* REPLACE (0 AS c)))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT 'if, false';
SELECT if(0, max(* REPLACE (10 + c AS c)), max(c))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
ORDER BY ALL
SETTINGS group_by_use_nulls = 0;
SELECT if(0, max(* REPLACE (10 + c AS c)), max(c))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT 'multiIf';
SELECT multiIf(0, max(* REPLACE (10 + c AS c)), 1, max(c), max(* REPLACE (0 AS c)))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
ORDER BY ALL
SETTINGS group_by_use_nulls = 0;
SELECT multiIf(0, max(* REPLACE (10 + c AS c)), 1, max(c), max(* REPLACE (0 AS c)))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT 'multiIf, else';
SELECT multiIf(0, max(* REPLACE (0 AS c)), max(c))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
ORDER BY ALL
SETTINGS group_by_use_nulls = 0;
SELECT multiIf(0, max(* REPLACE (0 AS c)), max(c))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;
