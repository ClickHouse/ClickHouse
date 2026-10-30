-- A comparison that is a GROUP BY key is read from the aggregation result, so the OR/AND chain rewrites keep it whole.
-- Random settings limits: optimize_and_compare_chain=(1, None)

SELECT 'or';
SELECT (x = 1) OR (x = 2) OR (x = 3) AS e, count() FROM (SELECT number % 4 AS x FROM numbers(8)) GROUP BY x = 1, x = 2, x = 3 ORDER BY ALL;
SELECT x = 1 AS a, x = 2 AS b, x = 3 AS c, a OR b OR c FROM (SELECT number % 4 AS x FROM numbers(8)) GROUP BY a, b, c ORDER BY ALL SETTINGS enable_identifier_resolve_cache = 1;
SELECT (s = '1') OR (s = '2') AS e, count() FROM (SELECT toLowCardinality(toString(number % 3)) AS s FROM numbers(12)) GROUP BY s = '1', s = '2' ORDER BY ALL;
SELECT countIf(e), count() FROM (SELECT x, (x = 1) OR (x = 2) OR (x = 3) OR (x = 4) AS e FROM (SELECT number % 6 AS x FROM numbers(12)) GROUP BY ROLLUP(x, x = 1));
SELECT count() FROM (SELECT number % 4 AS x FROM numbers(8)) GROUP BY if((x = 1) OR (x = 2) OR (x = 3), x, 0) = 1, if((x = 1) OR (x = 2) OR (x = 3), x, 0) = 2, if((x = 1) OR (x = 2) OR (x = 3), x, 0) = 3 HAVING (if((x = 1) OR (x = 2) OR (x = 3), x, 0) = 1) OR (if((x = 1) OR (x = 2) OR (x = 3), x, 0) = 2) OR (if((x = 1) OR (x = 2) OR (x = 3), x, 0) = 3) ORDER BY ALL;
SELECT ((x = 1) = 1) OR ((x = 2) = 1) OR ((x = 3) = 1) AS e, count() FROM (SELECT number % 4 AS x FROM numbers(8)) GROUP BY (x = 1) = 1, (x = 2) = 1, (x = 3) = 1 ORDER BY ALL;
SELECT (x = 1) OR (x = 2) OR (x = 3) AS e, count() FROM (SELECT number % 4 AS x FROM numbers(8)) GROUP BY e, if(e, x, 0) = 1 ORDER BY ALL SETTINGS enable_identifier_resolve_cache = 1;
SELECT countIf(e), count() FROM (SELECT x, (x = 1) OR (x = 2) OR (x = 3) AS e FROM (SELECT number % 4 AS x FROM numbers(8)) GROUP BY ROLLUP(x, e, if(e, x, 0) = 1)) SETTINGS enable_identifier_resolve_cache = 1;

SELECT 'or, group_by_use_nulls';
SELECT (x = 1) OR (x = 2) OR (x = 3) AS e, count() FROM (SELECT number % 4 AS x FROM numbers(8)) GROUP BY ROLLUP(x = 1, x = 2, x = 3) ORDER BY ALL SETTINGS group_by_use_nulls = 1;
SELECT countIf(e IS NULL), count() FROM (SELECT x, (x = 1) OR (x = 2) OR (x = 3) AS e FROM (SELECT number % 4 AS x FROM numbers(8)) GROUP BY ROLLUP(x, x = 1, x = 2, x = 3)) SETTINGS group_by_use_nulls = 1;
SELECT countIf(e IS NULL), count() FROM (SELECT x, (x = 1) OR (x = 2) OR (x = 3) AS e FROM (SELECT number % 4 AS x FROM numbers(8)) GROUP BY ROLLUP(x = 1, x = 2, x = 3, x)) SETTINGS group_by_use_nulls = 1;
SELECT countIf(e IS NULL), count() FROM (SELECT x, (x = 1) OR (x = 2) OR (x = 3) AS e FROM (SELECT number % 4 AS x FROM numbers(8)) GROUP BY ROLLUP(x, e, if(e, x, 0) = 1)) SETTINGS enable_identifier_resolve_cache = 1, group_by_use_nulls = 1;

SELECT 'and';
SELECT (x != 1) AND (x != 2) AND (x != 3) AS e, count() FROM (SELECT number % 4 AS x FROM numbers(8)) GROUP BY x != 1, x != 2, x != 3 ORDER BY ALL;
SELECT (x > 1.5) AND (x < 3.5) AS e, count() FROM (SELECT number % 6 AS x FROM numbers(12)) GROUP BY x > 1.5, x < 3.5 ORDER BY ALL;
SELECT (x >= 1) AND (x != 1) AS e, count() FROM (SELECT number % 6 AS x FROM numbers(12)) GROUP BY x >= 1, x != 1 ORDER BY ALL;
SELECT (t1.a = t2.b) AND (t2.b = 4) AS e, count() FROM (SELECT number % 7 AS a, number % 3 AS k FROM numbers(21)) AS t1 JOIN (SELECT number % 5 AS b, number % 3 AS k FROM numbers(15)) AS t2 ON t1.k = t2.k GROUP BY t1.a = t2.b, t2.b = 4 ORDER BY ALL;
SELECT ((t1.a = t2.b) AND (t2.b = 4)) AND (count() > 0) AS e, count() FROM (SELECT number % 7 AS a, number % 3 AS k FROM numbers(21)) AS t1 JOIN (SELECT number % 5 AS b, number % 3 AS k FROM numbers(15)) AS t2 ON t1.k = t2.k GROUP BY (t1.a = t2.b) AND (t2.b = 4) ORDER BY ALL;
SELECT count(), sum(c) FROM (SELECT count() AS c FROM (SELECT number % 4 AS a, number % 3 AS k FROM numbers(12)) AS t1 JOIN (SELECT number % 5 AS b, number % 3 AS k FROM numbers(15)) AS t2 ON t1.k = t2.k GROUP BY t1.a = 1, t1.a = 2, t1.a = 3, t2.b HAVING (((t1.a = 1) OR (t1.a = 2) OR (t1.a = 3)) < t2.b) AND (t2.b < 5));
SELECT ((x != 1) = 1) AND ((x != 2) = 1) AND ((x != 3) = 1) AS e, count() FROM (SELECT number % 4 AS x FROM numbers(8)) GROUP BY (x != 1) = 1, (x != 2) = 1, (x != 3) = 1 ORDER BY ALL;
SELECT ((x > 1.5) = 1) AND ((x < 2.5) = 1) AS e, count() FROM (SELECT number % 4 AS x FROM numbers(8)) GROUP BY (x > 1.5) = 1, (x < 2.5) = 1 ORDER BY ALL;
SELECT (x < y) AND (y < 5) AS e, count() FROM (SELECT number % 4 AS x, number % 6 AS y FROM numbers(12)) GROUP BY ROLLUP((x < y) AND (y < 5)) ORDER BY e SETTINGS group_by_use_nulls = 1;
SELECT (x = 1) AND (x = 2) AS e, count() FROM (SELECT number % 4 AS x FROM numbers(8)) GROUP BY ROLLUP((x = 1) AND (x = 2)) ORDER BY e SETTINGS group_by_use_nulls = 1;
SELECT (((x < y) AND (y < 5)) = 1) AND (x < 3) AS e, count() FROM (SELECT number % 4 AS x, number % 6 AS y FROM numbers(12)) GROUP BY (((x < y) AND (y < 5)) = 1) AND (x < 3) ORDER BY ALL;
SELECT countIf(e), count() FROM (SELECT x, y, (((x < y) AND (y < 5)) = 1) AND (x < 3) AS e FROM (SELECT number % 4 AS x, number % 6 AS y FROM numbers(12)) GROUP BY ROLLUP(x, y, (((x < y) AND (y < 5)) = 1) AND (x < 3)) ORDER BY ALL);
SELECT ((((x > 0) AND (x > 1)) AND (y > 0)) OR ((x > 1) AND (y < 4))) AND (y < 5) AS e, count() FROM (SELECT number % 4 AS x, number % 5 AS y FROM numbers(12)) WHERE e GROUP BY e ORDER BY ALL SETTINGS enable_identifier_resolve_cache = 1, optimize_extract_common_expressions = 1;
SELECT countIf(e), count() FROM (SELECT x, y, ((((x > 0) AND (x > 1)) AND (y > 0)) OR ((x > 1) AND (y < 4))) AND (y < 5) AS e FROM (SELECT number % 4 AS x, number % 5 AS y FROM numbers(12)) WHERE e GROUP BY ROLLUP(x, y, e) ORDER BY ALL) SETTINGS enable_identifier_resolve_cache = 1, optimize_extract_common_expressions = 1;
SELECT toUInt8((x < y) AND (y < 5)) AS e, count() FROM (SELECT number % 4 AS x, number % 6 AS y FROM numbers(12)) GROUP BY e, if(e, x, 0) = 1 ORDER BY ALL SETTINGS enable_identifier_resolve_cache = 1;

SELECT 'still rewritten';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT (x = 1) OR (x = 2) OR (x = 3), count() FROM (SELECT number % 4 AS x, number % 3 AS y FROM numbers(12)) GROUP BY x, y = 0) WHERE explain ILIKE '%function_name: in,%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT (x = 1) OR (x = 2) OR (x = 3) OR (x = 4), count() FROM (SELECT number % 6 AS x FROM numbers(12)) GROUP BY ROLLUP(x, x = 1)) WHERE explain ILIKE '%function_name: in,%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT (x != 1) AND (x != 2) AND (x != 3), count() FROM (SELECT number % 4 AS x FROM numbers(8)) GROUP BY x) WHERE explain ILIKE '%function_name: notIn,%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT (x > 1.5) AND (x < 3.5), count() FROM (SELECT number % 6 AS x FROM numbers(12)) GROUP BY x) WHERE explain ILIKE '%function_name: greaterOrEquals,%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT (t1.a = t2.b) AND (t2.b = 4), count() FROM (SELECT number % 7 AS a, number % 3 AS k FROM numbers(21)) AS t1 JOIN (SELECT number % 5 AS b, number % 3 AS k FROM numbers(15)) AS t2 ON t1.k = t2.k GROUP BY t1.a, t2.b) WHERE explain ILIKE '%function_name: equals,%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM (SELECT number % 6 AS x, number % 5 AS y FROM numbers(30)) WHERE ((x < y) AND (y < 5)) AND (x > 0) GROUP BY (x = 1) AND (y = 1)) WHERE explain ILIKE '%function_name: indexHint%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM (SELECT number % 7 AS a, number % 3 AS k FROM numbers(21)) AS t1 JOIN (SELECT number % 5 AS b, number % 3 AS k FROM numbers(15)) AS t2 ON t1.k = t2.k GROUP BY t1.a, t2.b, t1.k = 0 HAVING (t1.a < t2.b) AND (t2.b < 5)) WHERE explain ILIKE '%function_name: less,%';
