-- A matcher replaces a GROUP BY key at most once: `* REPLACE (d AS c, 1 AS d)` turns `c` into `d`, not into
-- the positional argument `1`, so the unrelated `APPLY` stays deferred until the Nullable GROUP BY keys are known.
SELECT t.COLUMNS('^d$') APPLY (x -> getSubcolumn(x, 'null')), * REPLACE (d AS c, 1 AS d)
FROM (SELECT number AS c, number AS d FROM numbers(3)) AS t
GROUP BY t.d, c WITH ROLLUP
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT '--';

-- The same without positional arguments.
SELECT t.COLUMNS('^d$') APPLY (x -> getSubcolumn(x, 'null')), * REPLACE (d AS c, 1 AS d)
FROM (SELECT number AS c, number AS d FROM numbers(3)) AS t
GROUP BY t.d, c WITH ROLLUP
ORDER BY ALL
SETTINGS group_by_use_nulls = 1, enable_positional_arguments = 0;

SELECT '--';

-- A matcher at the root of the body of a deferred `APPLY` lambda rewrites `HAVING`, as without `group_by_use_nulls`.
SELECT * APPLY (x -> COLUMNS('c') REPLACE (100 - c AS c))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
HAVING c > 98
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT '--';

SELECT * APPLY (x -> COLUMNS('c') REPLACE (100 - c AS c))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
HAVING c > 98
ORDER BY ALL
SETTINGS group_by_use_nulls = 0;

SELECT '--';

-- A nested matcher in a later `APPLY` of a chain that does not refer to the lambda argument is still expanded in advance.
SELECT * APPLY (x -> x + 1) APPLY (x -> max(COLUMNS('c') REPLACE (100 - c AS c)))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
HAVING c > 98
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT '--';

-- The input of a later `APPLY` is the result of the preceding transformer, not the original column.
SELECT * APPLY (x -> tuple(x)) APPLY (x -> tuple(x.*))
FROM numbers(3)
GROUP BY number WITH ROLLUP
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT '--';

SELECT * REPLACE (tuple(c) AS c) APPLY (x -> tuple(x.*))
FROM (SELECT number AS c FROM numbers(3))
GROUP BY c WITH ROLLUP
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;
