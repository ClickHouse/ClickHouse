-- With `group_by_use_nulls`: a duplicated alias with a matcher keeps its named window snapshots, a qualified `REPLACE` target
-- missing from the source of its qualifier and a `REPLACE` nested in a dropped matcher do not disable the deferred
-- transformers, and a failed speculative resolution leaves no alias lookup and no window registered.

SELECT 'dup matcher';
SELECT count(t.*) OVER w AS cnt, count(t.*) OVER w AS cnt
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
WINDOW w2 AS (PARTITION BY * REPLACE (intDiv(c, 2) AS c)),
       w AS (PARTITION BY c)
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;
SELECT 'dup matcher with';
WITH count(t.*) OVER w AS cnt SELECT count(t.*) OVER w AS cnt
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
WINDOW w2 AS (PARTITION BY * REPLACE (intDiv(c, 2) AS c)),
       w AS (PARTITION BY c)
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;
SELECT 'cross join qualifier';
SELECT t.c, t.* APPLY (x -> getSubcolumn(x, 'null')),
       s.* REPLACE (1 AS c)
FROM (SELECT number AS c FROM numbers(3)) AS t
CROSS JOIN (SELECT 1 AS d) AS s
GROUP BY c, d WITH ROLLUP
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;
SELECT 'nested dropped';
SELECT t.c,
       t.* APPLY (x -> getSubcolumn(x, 'null')),
       count(* REPLACE (identity(* REPLACE (1 AS c)) AS c))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY c WITH ROLLUP
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;
SELECT 'alias retry';
WITH getSubcolumn(t.c, 'null') AS nf
SELECT if(isNullable(t.c), identity(* APPLY (x -> nf)), toUInt8(7))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY t.c WITH ROLLUP
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;
SELECT 'window retry';
SELECT if(isNullable(t.c), identity(* REPLACE (count() OVER w AS c)), toUInt64(7))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY t.c WITH ROLLUP
WINDOW w AS (PARTITION BY getSubcolumn(t.c, 'null'))
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;
SELECT 'conflict still detected';
SELECT count(t.*) OVER w AS cnt, sum(t.*) OVER w AS cnt
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
WINDOW w2 AS (PARTITION BY * REPLACE (intDiv(c, 2) AS c)),
       w AS (PARTITION BY c)
ORDER BY ALL
SETTINGS group_by_use_nulls = 1; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
