-- A failed speculative resolution of a `REPLACE` transformer must not leave the CTE registered as being resolved,
-- otherwise the retry after GROUP BY does not find the CTE.
SET enable_analyzer = 1;

WITH f AS (SELECT getSubcolumn(t.c, 'null'))
SELECT if(isNullable(t.c),
          identity(* REPLACE ((SELECT * FROM f) AS c)),
          toUInt8(7))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY t.c WITH ROLLUP
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

WITH f AS (SELECT getSubcolumn(t.c, 'null'))
SELECT if(isNullable(t.c),
          identity(* REPLACE ((SELECT * FROM f) AS c)),
          toUInt8(7))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY t.c WITH ROLLUP
ORDER BY ALL
SETTINGS group_by_use_nulls = 0;

-- Many identical aliased projection items.
SELECT count() FROM (SELECT max(number) AS c, max(number) AS c, max(number) AS c FROM numbers(3) GROUP BY number WITH ROLLUP SETTINGS group_by_use_nulls = 1);
