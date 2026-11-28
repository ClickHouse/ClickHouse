-- With `group_by_use_nulls`, the result of a transformer of a matcher that is consumed by an aggregate function
-- of a following transformer is computed before the aggregation, from the GROUP BY keys in their original types.
-- The same result used outside of the aggregate function is computed from the Nullable keys.

SET enable_analyzer = 1;
SET group_by_use_nulls = 1;

SELECT '-- function, function';
SELECT * APPLY toInt64 APPLY sum FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;
SELECT * APPLY abs APPLY sum FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;

SELECT '-- lambda, function';
SELECT * APPLY (x -> toInt64(x)) APPLY sum FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;

SELECT '-- lambda, lambda';
SELECT * APPLY (x -> toInt64(x)) APPLY (x -> sum(x)) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;

SELECT '-- replace, function';
SELECT * REPLACE (toInt64(number) AS number) APPLY sum FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;

SELECT '-- a transformer after the aggregate function';
SELECT * APPLY toInt64 APPLY sum APPLY toString FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;

SELECT '-- inside and outside of the aggregate function';
SELECT * APPLY toInt64 APPLY (x -> tuple(sum(x), x, toTypeName(x))) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;
SELECT * APPLY (x -> x) APPLY (x -> tuple(sum(x), x)) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;
SELECT * REPLACE (number + 1 AS number) APPLY (x -> tuple(max(x), x)) APPLY toTypeName FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;

SELECT '-- a longer chain';
SELECT * APPLY toInt64 APPLY abs APPLY (x -> tuple(max(x), toTypeName(x))) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;
SELECT * APPLY toString APPLY (x -> (x, x)) APPLY (x -> max(x.1)) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;

SELECT '-- a window function is computed after the aggregation';
SELECT * APPLY toInt64 APPLY (x -> sum(x) OVER ()) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;

SELECT '-- aggregate functions in several transformers';
SELECT * APPLY sum APPLY (x -> tuple(x, count())) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;
SELECT * APPLY toInt64 APPLY (x -> tuple(sum(x), grouping(number), x)) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;

-- The transformers of a matcher qualified by the argument of a deferred `APPLY` lambda, which is not known yet, may contain
-- matchers that do not depend on the argument. Their `REPLACE` mappings must reach `HAVING` as without `group_by_use_nulls`.
SELECT '-- independent matchers in the transformers of a postponed matcher';

SELECT * APPLY (x -> tuple(x))
         APPLY (x -> tuple(x.* APPLY (y -> max(COLUMNS('c') REPLACE (100 - c AS c)))))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
HAVING c > 98
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT * APPLY (x -> tuple(x))
         APPLY (x -> tuple(x.* APPLY (y -> max(COLUMNS('c') REPLACE (100 - c AS c)))))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
HAVING c > 98
ORDER BY ALL
SETTINGS group_by_use_nulls = 0;

SELECT * APPLY (x -> tuple(x))
         APPLY (x -> tuple(x.* REPLACE (max(COLUMNS('c') REPLACE (100 - c AS c)) AS `1`)))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
HAVING c > 98
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT * APPLY (x -> tuple(x))
         APPLY (x -> tuple(x.* REPLACE (max(COLUMNS('c') REPLACE (100 - c AS c)) AS `1`)))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
HAVING c > 98
ORDER BY ALL
SETTINGS group_by_use_nulls = 0;

-- The alias of a projection item is defined first in `WITH`, by an expression that is equal to the item only after
-- the matcher is expanded. The alias in `QUALIFY` resolves the named window as the projection item does:
-- from its definition before the `REPLACE` transformer of the other window rewrote it.
SELECT '-- an alias defined by an equivalent expression in WITH';

WITH count(t.c) OVER w AS cnt
SELECT count(t.*) OVER w AS cnt
FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY GROUPING SETS ((t.c))
WINDOW w2 AS (PARTITION BY * REPLACE (intDiv(c, 2) AS c)),
       w AS (PARTITION BY c)
QUALIFY cnt = 1
ORDER BY ALL;

WITH count(t.c) OVER w AS cnt
SELECT count(t.*) OVER w AS cnt
FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY GROUPING SETS ((t.c))
WINDOW w2 AS (PARTITION BY * REPLACE (intDiv(c, 2) AS c)),
       w AS (PARTITION BY c)
QUALIFY cnt = 2
ORDER BY ALL;

-- Different definitions of the alias are still rejected.
WITH sum(t.c) OVER w AS cnt
SELECT count(t.*) OVER w AS cnt
FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY GROUPING SETS ((t.c))
WINDOW w2 AS (PARTITION BY * REPLACE (intDiv(c, 2) AS c)),
       w AS (PARTITION BY c)
QUALIFY cnt = 1
ORDER BY ALL; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
