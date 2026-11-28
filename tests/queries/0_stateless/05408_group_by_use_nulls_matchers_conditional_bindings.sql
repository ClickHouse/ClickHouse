-- Matchers in the arguments of `if` and `multiIf` under `group_by_use_nulls` are expanded in the same order as without it.

-- A foldable constant condition selects the branch, as a literal one: the `REPLACE` in the unreachable branch rewrites the selected one.
SELECT if(1 = 1, max(c), max(* REPLACE (0 AS c))) FROM (SELECT number AS c FROM numbers(3)) AS t GROUP BY GROUPING SETS ((t.c)) ORDER BY ALL SETTINGS group_by_use_nulls = 1;
SELECT multiIf(1 = 1, max(c), max(* REPLACE (0 AS c))) FROM (SELECT number AS c FROM numbers(3)) AS t GROUP BY GROUPING SETS ((t.c)) ORDER BY ALL SETTINGS group_by_use_nulls = 1;
SELECT multiIf(1 = 0, max(* REPLACE (0 AS c)), 2 > 1, max(c), max(c)) FROM (SELECT number AS c FROM numbers(3)) AS t GROUP BY GROUPING SETS ((t.c)) ORDER BY ALL SETTINGS group_by_use_nulls = 1;

-- A failed expansion of an unreachable branch does not keep the preceding GROUP BY keys from being converted to Nullable.
SELECT t.c, if(0, max(* REPLACE (missing AS c)), 7) FROM (SELECT number AS c FROM numbers(3)) AS t GROUP BY t.c WITH ROLLUP ORDER BY t.c NULLS LAST SETTINGS group_by_use_nulls = 1;
SELECT t.c, multiIf(0, max(* REPLACE (missing AS c)), 7) FROM (SELECT number AS c FROM numbers(3)) AS t GROUP BY t.c WITH ROLLUP ORDER BY t.c NULLS LAST SETTINGS group_by_use_nulls = 1;
SELECT t.c, if(0, max(* APPLY noSuchFunction), 7) FROM (SELECT number AS c FROM numbers(3)) AS t GROUP BY t.c WITH ROLLUP ORDER BY t.c NULLS LAST SETTINGS group_by_use_nulls = 1;

-- A lambda shadowing `if` or `multiIf` resolves its arguments from left to right.
WITH (cond, a, b) -> a AS if SELECT if(1, max(c), max(* REPLACE (0 AS c))) FROM (SELECT number AS c FROM numbers(3)) AS t GROUP BY GROUPING SETS ((t.c)) ORDER BY ALL SETTINGS group_by_use_nulls = 1;
WITH (cond, a, b) -> a AS multiIf SELECT multiIf(1, max(c), max(* REPLACE (0 AS c))) FROM (SELECT number AS c FROM numbers(3)) AS t GROUP BY GROUPING SETS ((t.c)) ORDER BY ALL SETTINGS group_by_use_nulls = 1;
