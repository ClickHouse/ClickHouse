-- A GROUP BY key that contains another key in several places, under ROLLUP / CUBE / GROUPING SETS with group_by_use_nulls.
-- The identifier resolve cache makes those references one node; the projection must still read the key itself.

SET group_by_use_nulls = 1;
SET enable_identifier_resolve_cache = 1;
-- The reference has the type names of the `String` result of `if` and `multiIf`.
SET optimize_if_transform_const_strings_to_lowcardinality = 0;

SELECT x, if((x = 1) OR (x = 2) OR (x = 3), 'a', 'b') AS e, toTypeName(e), count() FROM (SELECT number % 4 AS x FROM numbers(8)) GROUP BY ROLLUP(e, x) ORDER BY ALL;
SELECT (x = 1) OR (x = 2) OR (x = 3) AS e, toTypeName(e), count() FROM (SELECT number % 4 AS x FROM numbers(8)) GROUP BY GROUPING SETS ((e), (x)) ORDER BY ALL;
SELECT x, multiIf(x = 1, 'a', x = 2, 'b', 'c') AS e, toTypeName(e), count() FROM (SELECT number % 4 AS x FROM numbers(8)) GROUP BY CUBE(e, x) ORDER BY ALL;
SELECT x, if(x = 1 OR x = 2, 'a', 'b') AS e, toTypeName(e), count() FROM (SELECT number % 4 AS x FROM numbers(8)) GROUP BY ROLLUP(if(x = 1 OR x = 2, 'a', 'b'), x) ORDER BY ALL;
SELECT x, if(x = 1 OR x = 2, 'a', 'b') AS e, count() FROM (SELECT number % 4 AS x FROM numbers(8)) GROUP BY ROLLUP(e, x) HAVING e IS NULL OR e = 'a' ORDER BY ALL;
SELECT x, x + 1 AS y, if(y = 2 OR y = 3, 'a', 'b') AS e, toTypeName(e), count() FROM (SELECT number % 4 AS x FROM numbers(8)) GROUP BY ROLLUP(e, x) ORDER BY ALL;
SELECT x, x = 1 AS c, multiIf(c, 'a', c OR x = 2, 'b', 'c') AS e, toTypeName(e), count() FROM (SELECT number % 4 AS x FROM numbers(8)) GROUP BY ROLLUP(e, c, x) ORDER BY ALL;
SELECT countIf(e IS NULL), countIf(e = 'a'), count() FROM (SELECT x, if((x = 1) OR (x = 2) OR (x = 3), 'a', 'b') AS e FROM (SELECT number % 4 AS x FROM numbers(8)) GROUP BY ROLLUP(e, x)) SETTINGS optimize_min_equality_disjunction_chain_length = 1000;
