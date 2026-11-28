-- A `REPLACE` transformer that rewrites a GROUP BY key into a non-constant expression does not make the key positional,
-- so the unrelated `APPLY` transformers stay deferred until the Nullable GROUP BY keys are known.
SELECT t.c, t.* APPLY (x -> getSubcolumn(x, 'null')), * REPLACE (t.c AS c)
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY c WITH ROLLUP
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT '--';

-- The same with a positional argument referring to a different projection item.
SELECT t.c, t.* APPLY (x -> getSubcolumn(x, 'null')), * REPLACE (t.c AS c)
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY 1, c WITH ROLLUP
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT '--';

-- The matchers inside a deferred `APPLY` transformer register their `REPLACE` mappings in the other clauses:
-- in the body of a lambda and in the parameters of a function.
SELECT * APPLY (x -> max(COLUMNS('c') REPLACE (100 - c AS c)))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
HAVING c > 98
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT '--';

SELECT * APPLY (x -> max(COLUMNS('c') REPLACE (100 - c AS c)))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
HAVING c > 98
ORDER BY ALL
SETTINGS group_by_use_nulls = 0;

SELECT '--';

SELECT * APPLY quantile(toFloat64(COLUMNS('c') REPLACE (0.5 AS c)))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
HAVING c = 0.5
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT '--';

SELECT * APPLY quantile(toFloat64(COLUMNS('c') REPLACE (0.5 AS c)))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
HAVING c = 0.5
ORDER BY ALL
SETTINGS group_by_use_nulls = 0;
