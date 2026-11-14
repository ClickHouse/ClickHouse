-- With `group_by_use_nulls`, a `REPLACE` transformer that turns a GROUP BY key into a positional argument
-- has to be found in the `COLUMNS` lists with compound or aliased items and in the named windows.
SET enable_analyzer = 1;
SET enable_positional_arguments = 1;

-- A qualified `COLUMNS` list item.
SELECT COLUMNS(t.c) REPLACE (1 AS c), count() AS n
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((c))
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT COLUMNS(t.c) REPLACE (1 AS c), count() AS n
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((c))
ORDER BY ALL
SETTINGS group_by_use_nulls = 0;

-- An aliased `COLUMNS` list item.
WITH t.c AS d
SELECT COLUMNS(d) REPLACE (1 AS c), count() AS n
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((c))
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

-- An unqualified `COLUMNS` list item.
SELECT COLUMNS(c) REPLACE (1 AS c), count() AS n
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((c))
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

-- A `REPLACE` in a named window and the same window written inline.
SELECT * APPLY (x -> x % 2), count() OVER w
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((c))
WINDOW w AS (PARTITION BY * REPLACE (1 AS c))
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT * APPLY (x -> x % 2), count() OVER (PARTITION BY * REPLACE (1 AS c))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((c))
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT * APPLY (x -> x % 2), count() OVER w
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((c))
WINDOW w AS (PARTITION BY * REPLACE (1 AS c))
ORDER BY ALL
SETTINGS group_by_use_nulls = 0;
