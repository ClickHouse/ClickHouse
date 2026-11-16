-- A constant column of a subquery whose projection is merged into the reordered join must keep its value
-- in the rows an outer join pads.

SET query_plan_optimize_join_order_algorithm = 'dpsub,greedy';
SET query_plan_optimize_join_order_conflict_detector = 'c';
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_merge_expression_into_join = 1;
SET join_use_nulls = 0;

SELECT r.h
FROM numbers(0) AS outer_l
RIGHT ANTI JOIN (
    SELECT toInt64(123) AS h
    FROM (SELECT 1 FROM numbers(1) ARRAY JOIN [0] AS a) AS l
    RIGHT JOIN (SELECT 1) AS r ON TRUE
) AS r ON FALSE
SETTINGS allow_experimental_join_condition = 1;

SELECT r.h FROM numbers(0) AS l RIGHT JOIN (SELECT toUInt64(123) AS h FROM (SELECT 1) AS a CROSS JOIN (SELECT 1) AS b) AS r ON l.number = r.h SETTINGS query_plan_join_swap_table = 'false';
SELECT r.x, r.s, r.lc, r.n, r.arr, r.tup FROM numbers(0) AS l RIGHT JOIN (SELECT a.x AS x, 'abc' AS s, toLowCardinality('abc') AS lc, CAST(7 AS Nullable(Int64)) AS n, [1, 2] AS arr, (1, 'z') AS tup FROM (SELECT toUInt64(1) AS x) AS a CROSS JOIN (SELECT 1) AS b) AS r ON l.number = r.x SETTINGS query_plan_join_swap_table = 'false';
SELECT r.h, toTypeName(r.h) FROM numbers(0) AS l RIGHT JOIN (SELECT toUInt64(123) AS h FROM (SELECT 1) AS a CROSS JOIN (SELECT 1) AS b) AS r ON l.number = r.h SETTINGS query_plan_join_swap_table = 'false', join_use_nulls = 1;
SELECT r.s FROM numbers(10) AS l RIGHT ANTI JOIN (SELECT a.x AS x, 'abc' AS s FROM (SELECT toUInt64(100) AS x) AS a CROSS JOIN (SELECT 1) AS b) AS r ON l.number = r.x;

SELECT l.h FROM (SELECT toUInt64(123) AS h FROM (SELECT 1) AS a CROSS JOIN (SELECT 1) AS b) AS l LEFT JOIN numbers(0) AS r ON r.number = l.h SETTINGS query_plan_join_swap_table = 'true';
SELECT l.h FROM (SELECT toUInt64(123) AS h FROM (SELECT 1) AS a CROSS JOIN (SELECT 1) AS b) AS l LEFT JOIN numbers(0) AS r ON r.number = l.h SETTINGS query_plan_join_swap_table = 'true', query_plan_optimize_join_order_algorithm = 'greedy', query_plan_optimize_join_order_conflict_detector = '';

SELECT l.x, l.h FROM (SELECT a.x AS x, 'abc' AS h FROM (SELECT toUInt64(number) AS x FROM numbers(3)) AS a INNER JOIN (SELECT toUInt64(number) AS y FROM numbers(3)) AS b ON a.x = b.y) AS l INNER JOIN (SELECT number FROM numbers(1000)) AS r ON r.number = l.x ORDER BY ALL;
SELECT l.x, l.h FROM (SELECT a.x AS x, 'abc' AS h FROM (SELECT toUInt64(number) AS x FROM numbers(3)) AS a INNER JOIN (SELECT toUInt64(number) AS y FROM numbers(3)) AS b ON a.x = b.y) AS l LEFT JOIN (SELECT number FROM numbers(1000) WHERE number > 100) AS r ON r.number = l.x ORDER BY ALL;

SELECT l.number, r.h FROM numbers(5, 1) AS l FULL JOIN (SELECT toUInt64(123) AS h FROM (SELECT 1) AS a CROSS JOIN (SELECT 1) AS b) AS r ON l.number = r.h ORDER BY ALL SETTINGS query_plan_join_swap_table = 'false';
SELECT count() FROM numbers(0) AS l RIGHT JOIN (SELECT toUInt64(123) AS h FROM (SELECT 1) AS a CROSS JOIN (SELECT 1) AS b) AS r ON l.number = r.h SETTINGS query_plan_join_swap_table = 'false';
SELECT count() FROM (SELECT number AS x FROM numbers(3)) AS a, (SELECT number AS y FROM numbers(4)) AS b, (SELECT number AS z FROM numbers(5)) AS c;
