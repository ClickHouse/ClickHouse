-- The expressions produced by the `APPLY` and `REPLACE` transformers of the projection matchers are resolved
-- against the Nullable GROUP BY keys under `group_by_use_nulls`, as the same expressions written explicitly.

SET enable_analyzer = 1;
SET group_by_use_nulls = 1;

SELECT getSubcolumn(c, 'null') FROM (SELECT number AS c FROM numbers(3)) AS t GROUP BY t.c WITH ROLLUP ORDER BY ALL;
SELECT * APPLY (x -> getSubcolumn(x, 'null')) FROM (SELECT number AS c FROM numbers(3)) AS t GROUP BY t.c WITH ROLLUP ORDER BY ALL;
SELECT * REPLACE (getSubcolumn(c, 'null') AS c) FROM (SELECT number AS c FROM numbers(3)) AS t GROUP BY t.c WITH ROLLUP ORDER BY ALL;

-- A positional argument of GROUP BY refers to the transformed projection item, which is a key resolved against the source column.
SELECT * APPLY (x -> x + 1) FROM numbers(3) GROUP BY 1 WITH ROLLUP ORDER BY ALL;

-- A lambda bound to the name `grouping` is called instead of the builtin function, so the key is Nullable.
WITH (x -> isNull(x)) AS grouping SELECT * APPLY grouping FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;
WITH (x -> isNull(x)) AS grouping SELECT * APPLY (x -> grouping(x)) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;
SELECT * APPLY (x -> grouping(x)) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;
