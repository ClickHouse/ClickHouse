-- With `group_by_use_nulls`, a `REPLACE` transformer that cannot rewrite `GROUP BY` during the expansion of the projection
-- must not force the other transformers to be resolved against the non-Nullable source columns.
SET enable_analyzer = 1;
SET enable_positional_arguments = 1;

-- A named window that is not referred to.
SELECT t.c, t.* APPLY (x -> getSubcolumn(x, 'null'))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY c WITH ROLLUP
WINDOW unused AS (PARTITION BY * REPLACE (1 AS c))
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

-- A named window that is referred to through another named window.
SELECT * APPLY (x -> x % 2), count() OVER w2
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((c))
WINDOW w AS (PARTITION BY * REPLACE (1 AS c)), w2 AS (w)
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

-- A qualified `COLUMNS` list item that does not produce the replaced column.
SELECT t.c, t.COLUMNS('^c$') APPLY (x -> getSubcolumn(x, 'null')),
       COLUMNS(t.d) REPLACE (1 AS c)
FROM (SELECT number AS c, number AS d FROM numbers(3)) AS t
GROUP BY c, d WITH ROLLUP
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;
