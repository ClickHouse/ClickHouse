-- The first expansion of the projection matchers under `group_by_use_nulls` defers the expressions
-- produced by the transformers, and this must not change which `REPLACE` mappings rewrite the other clauses.

SET enable_analyzer = 1;

-- A `REPLACE` transformer in an unreachable branch with an invalid expression must not rewrite `HAVING` or `WHERE`.
SELECT 'dead branch, HAVING';
SELECT if(0, max(* REPLACE (missing AS c)), 7)
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
HAVING c > 0
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT 'dead branch, WHERE';
SELECT if(0, max(* REPLACE (missing AS c)), 7)
FROM (SELECT number AS c FROM numbers(3)) AS t
WHERE c > 0
GROUP BY GROUPING SETS ((t.c))
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

-- A positional argument of GROUP BY refers to another projection item: the transformer expressions are still deferred.
SELECT 'positional, APPLY';
SELECT t.c, * APPLY (x -> getSubcolumn(x, 'null'))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY 1 WITH ROLLUP
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT 'positional, REPLACE';
SELECT t.c, * REPLACE (getSubcolumn(c, 'null') AS c)
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY 1 WITH ROLLUP
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

-- A positional argument referring to the matcher itself keeps the pre-aggregation expression as the key.
SELECT 'positional, the matcher';
SELECT * APPLY (x -> x + 1)
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY 1 WITH ROLLUP
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

-- A matcher inside the expression of a deferred `REPLACE` transformer registers its mappings as without the setting.
SELECT 'nested REPLACE, 0';
SELECT * REPLACE (max(* REPLACE (100 - c AS c)) AS c)
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
HAVING c > 98
ORDER BY ALL
SETTINGS group_by_use_nulls = 0;

SELECT 'nested REPLACE, 1';
SELECT * REPLACE (max(* REPLACE (100 - c AS c)) AS c)
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
HAVING c > 98
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;
