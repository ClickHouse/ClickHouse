-- The body of a deferred `APPLY` lambda that follows another deferred transformer uses both its argument and
-- an independent matcher. The `REPLACE` mapping of the independent matcher must reach `HAVING` as without `group_by_use_nulls`,
-- while the matchers qualified by the argument wait for the result of the preceding transformer.

SET enable_analyzer = 1;

SELECT * APPLY (x -> tuple(x))
         APPLY (x -> tuple(x, max(COLUMNS('c') REPLACE (100 - c AS c))))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
HAVING c > 98
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT * APPLY (x -> tuple(x))
         APPLY (x -> tuple(x, max(COLUMNS('c') REPLACE (100 - c AS c))))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
HAVING c > 98
ORDER BY ALL
SETTINGS group_by_use_nulls = 0;

-- A matcher qualified by the argument is expanded for the result of the preceding transformer.
SELECT * APPLY (x -> tuple(x))
         APPLY (x -> tuple(x.*, max(COLUMNS('c') REPLACE (100 - c AS c))))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
HAVING c > 98
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT * APPLY (x -> tuple(x))
         APPLY (x -> tuple(x.*, max(COLUMNS('c') REPLACE (100 - c AS c))))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
HAVING c > 98
ORDER BY ALL
SETTINGS group_by_use_nulls = 0;
