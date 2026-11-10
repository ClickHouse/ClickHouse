-- With `group_by_use_nulls`, a `REPLACE` target that exists only on the side of a SEMI/ANTI JOIN hidden from `*`
-- must not be treated as a positional `GROUP BY` key: the unrelated `APPLY` is resolved only after `GROUP BY`.

SET enable_analyzer = 1;

WITH t.c AS d
SELECT t.c, t.* APPLY (x -> getSubcolumn(x, 'null')), * REPLACE (1 AS d)
FROM (SELECT number AS c FROM numbers(3)) AS t
LEFT SEMI JOIN (SELECT 1 AS d) AS s ON 1
GROUP BY c, d WITH ROLLUP
ORDER BY t.c NULLS LAST
SETTINGS group_by_use_nulls = 1, semi_join_include_columns_from_both_sides = 0;

SELECT '--';

WITH t.c AS d
SELECT t.c, t.* APPLY (x -> getSubcolumn(x, 'null')), * REPLACE (1 AS d)
FROM (SELECT number AS c FROM numbers(3)) AS t
LEFT ANTI JOIN (SELECT 1 AS d) AS s ON 0
GROUP BY c, d WITH ROLLUP
ORDER BY t.c NULLS LAST
SETTINGS group_by_use_nulls = 1, anti_join_include_columns_from_both_sides = 0;

SELECT '--';

-- The hidden side is the left one for RIGHT SEMI JOIN.
WITH s.c AS d
SELECT s.c, s.* APPLY (x -> getSubcolumn(x, 'null')), * REPLACE (1 AS d)
FROM (SELECT 1 AS d) AS t
RIGHT SEMI JOIN (SELECT number AS c FROM numbers(3)) AS s ON 1
GROUP BY c, d WITH ROLLUP
ORDER BY s.c NULLS LAST
SETTINGS group_by_use_nulls = 1, semi_join_include_columns_from_both_sides = 0;
