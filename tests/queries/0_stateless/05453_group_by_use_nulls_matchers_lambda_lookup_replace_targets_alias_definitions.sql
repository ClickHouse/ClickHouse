SET enable_analyzer = 1;

-- A scalar alias named as a builtin function does not make it a lambda: `count(*)` keeps counting all rows.
SELECT a AS count, count(*)
FROM values('a Nullable(UInt8)', (NULL), (1)) AS t
GROUP BY a WITH ROLLUP
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT a AS count, count(*)
FROM values('a Nullable(UInt8), b UInt8', (NULL, 1), (1, 2)) AS t
GROUP BY a WITH ROLLUP
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

-- A lambda of the outer query is not visible with `enable_global_with_statement = 0`.
WITH (x -> 1) AS count
SELECT *
FROM
(
    SELECT count(*)
    FROM (SELECT number AS a, number AS b FROM numbers(3))
    GROUP BY a WITH ROLLUP
    ORDER BY ALL
    SETTINGS group_by_use_nulls = 1, enable_global_with_statement = 0
);

-- The same for `grouping`.
SELECT a AS grouping, * APPLY x -> grouping(x)
FROM (SELECT number AS a FROM numbers(2))
GROUP BY a WITH ROLLUP
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

-- A `REPLACE` target removed by the preceding `EXCEPT` of the same matcher does not rewrite the `GROUP BY` key,
-- so the unrelated `APPLY` is resolved against the Nullable key.
SELECT t.c, * APPLY (x -> getSubcolumn(x, 'null')), * EXCEPT(c) REPLACE (1 AS c)
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY c WITH ROLLUP
ORDER BY 1, 2
SETTINGS group_by_use_nulls = 1;

-- The same for a target that the matcher does not match.
SELECT t.c, * APPLY (x -> getSubcolumn(x, 'null')), COLUMNS('d') REPLACE (1 AS c)
FROM (SELECT number AS c, number AS d FROM numbers(3)) AS t
GROUP BY c, d WITH ROLLUP
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

-- A conflicting definition of the alias is still detected.
WITH 42 AS cnt
SELECT count() OVER w AS cnt
FROM numbers(3)
GROUP BY GROUPING SETS ((number))
WINDOW w AS (PARTITION BY number)
ORDER BY ALL
SETTINGS group_by_use_nulls = 1; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }

SELECT count() OVER w AS cnt, sum(number) OVER w AS cnt
FROM numbers(3)
GROUP BY GROUPING SETS ((number))
WINDOW w AS (PARTITION BY number)
ORDER BY ALL
SETTINGS group_by_use_nulls = 1; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }

-- Identical definitions are allowed.
SELECT count() OVER w AS cnt, count() OVER w AS cnt
FROM numbers(3)
GROUP BY GROUPING SETS ((number))
WINDOW w AS (PARTITION BY number)
ORDER BY cnt
SETTINGS group_by_use_nulls = 1;
