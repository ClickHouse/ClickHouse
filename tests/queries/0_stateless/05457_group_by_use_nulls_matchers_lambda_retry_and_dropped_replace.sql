-- With `group_by_use_nulls`, a transformer valid only for the Nullable GROUP BY keys fails while the matchers
-- are expanded before GROUP BY, and its lambda must be resolvable again afterwards (not reported as recursive).
SELECT if(isNullable(t.c), identity(* APPLY (x -> getSubcolumn(x, 'null'))), toUInt8(7))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY t.c WITH ROLLUP
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT multiIf(isNullable(t.c), identity(* APPLY (x -> getSubcolumn(x, 'null'))), toUInt8(7))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY t.c WITH ROLLUP
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

-- A `REPLACE` that cannot rewrite GROUP BY does not prevent deferring the unrelated `APPLY` until after GROUP BY:
-- the unqualified matcher of `count` is dropped,
SELECT t.c, t.* APPLY (x -> getSubcolumn(x, 'null')), count(* REPLACE (1 AS c))
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY c WITH ROLLUP
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

-- and the target is not a column of the source.
SELECT t.c, t.* APPLY (x -> getSubcolumn(x, 'null')), t.* REPLACE (1 AS d)
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY c WITH ROLLUP
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT t.c, * APPLY (x -> getSubcolumn(x, 'null')), * REPLACE (1 AS d)
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY c WITH ROLLUP
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;
