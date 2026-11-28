-- With `group_by_use_nulls`, a `REPLACE` transformer in the body of a lambda passed to an ordinary function does not
-- rewrite `GROUP BY`, because the lambda is resolved with the rest of the projection, after `GROUP BY`. So it must
-- not keep the `APPLY` transformer of another matcher from being resolved after `GROUP BY`, where the keys are Nullable.

SET enable_analyzer = 1;
SET group_by_use_nulls = 1;

SELECT t.c, t.* APPLY (x -> getSubcolumn(x, 'null')), arrayMap(x -> (x, identity(COLUMNS('c') REPLACE (1 AS c))), [1])
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY c WITH ROLLUP
ORDER BY ALL;

-- The same lambda bound to an alias.
WITH x -> (x, identity(COLUMNS('c') REPLACE (1 AS c))) AS f
SELECT t.c, t.* APPLY (x -> getSubcolumn(x, 'null')), arrayMap(f, [1])
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY c WITH ROLLUP
ORDER BY ALL;

-- A lambda nested in the arguments of a function.
SELECT t.c, t.* APPLY (x -> getSubcolumn(x, 'null')), length(arrayFilter(x -> (COLUMNS('c') REPLACE (1 AS c)) = 1, [1, 2]))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY c WITH ROLLUP
ORDER BY ALL;
