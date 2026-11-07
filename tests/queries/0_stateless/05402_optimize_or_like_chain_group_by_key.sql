-- Random settings limits: optimize_or_like_chain=(1, None)

-- After GROUP BY, a branch of an OR chain that is itself a GROUP BY key is read from the aggregation result.

SELECT 'substring keys';
SELECT * FROM (SELECT (s LIKE '%1%') OR (s LIKE '%2%') OR (s LIKE '%3%') OR (s LIKE '%4%') AS e, count() AS c FROM (SELECT toString(number % 6) AS s FROM numbers(12)) GROUP BY s LIKE '%1%', s LIKE '%2%', s LIKE '%3%', s LIKE '%4%') ORDER BY ALL;
SELECT 'case-insensitive keys';
SELECT * FROM (SELECT (s ILIKE '%a%') OR (s ILIKE '%b%') OR (s ILIKE '%c%') OR (s ILIKE '%d%') AS e, count() AS c FROM (SELECT ['A', 'b', 'x', 'D', 'y', 'c'][number % 6 + 1] AS s FROM numbers(12)) GROUP BY s ILIKE '%a%', s ILIKE '%b%', s ILIKE '%c%', s ILIKE '%d%') ORDER BY ALL;
SELECT 'prefix keys';
SELECT * FROM (SELECT (s LIKE '0%') OR (s LIKE '1%') OR (s LIKE '2%') OR (s LIKE '3%') OR (s LIKE '4%') OR (s LIKE '5%') OR (s LIKE '6%') OR (s LIKE '7%') OR (s LIKE '8%') OR (s LIKE '9%') AS e, count() AS c FROM (SELECT toString(number % 13) AS s FROM numbers(26)) GROUP BY s LIKE '0%', s LIKE '1%', s LIKE '2%', s LIKE '3%', s LIKE '4%', s LIKE '5%', s LIKE '6%', s LIKE '7%', s LIKE '8%', s LIKE '9%') ORDER BY ALL;
SELECT 'HAVING';
SELECT * FROM (SELECT count() AS c FROM (SELECT toString(number % 6) AS s FROM numbers(12)) GROUP BY s LIKE '%1%', s LIKE '%2%', s LIKE '%3%', s LIKE '%4%' HAVING (s LIKE '%1%') OR (s LIKE '%2%') OR (s LIKE '%3%') OR (s LIKE '%4%')) ORDER BY ALL;
SELECT 'chain with another branch';
SELECT * FROM (SELECT (s LIKE '%1%') OR (s LIKE '%2%') OR (s LIKE '%3%') OR (s LIKE '%4%') OR (count() > 100) AS e, count() AS c FROM (SELECT toString(number % 6) AS s FROM numbers(12)) GROUP BY s LIKE '%1%', s LIKE '%2%', s LIKE '%3%', s LIKE '%4%') ORDER BY ALL;
SELECT 'aliases';
SELECT * FROM (SELECT s LIKE '%1%' AS a, s LIKE '%2%' AS b, s LIKE '%3%' AS c, s LIKE '%4%' AS d, a OR b OR c OR d AS e, count() AS n FROM (SELECT toString(number % 6) AS s FROM numbers(12)) GROUP BY a, b, c, d) ORDER BY ALL SETTINGS enable_identifier_resolve_cache = 0;
SELECT * FROM (SELECT s LIKE '%1%' AS a, s LIKE '%2%' AS b, s LIKE '%3%' AS c, s LIKE '%4%' AS d, a OR b OR c OR d AS e, count() AS n FROM (SELECT toString(number % 6) AS s FROM numbers(12)) GROUP BY a, b, c, d) ORDER BY ALL SETTINGS enable_identifier_resolve_cache = 1;
SELECT 'Nullable and LowCardinality columns';
SELECT * FROM (SELECT (s LIKE '%1%') OR (s LIKE '%2%') OR (s LIKE '%3%') OR (s LIKE '%4%') AS e, count() AS c FROM (SELECT if(number = 0, NULL, toString(number % 6)) AS s FROM numbers(12)) GROUP BY s LIKE '%1%', s LIKE '%2%', s LIKE '%3%', s LIKE '%4%') ORDER BY ALL;
SELECT * FROM (SELECT (s LIKE '%1%') OR (s LIKE '%2%') OR (s LIKE '%3%') OR (s LIKE '%4%') AS e, count() AS c FROM (SELECT toLowCardinality(toString(number % 6)) AS s FROM numbers(12)) GROUP BY s LIKE '%1%', s LIKE '%2%', s LIKE '%3%', s LIKE '%4%') ORDER BY ALL;
SELECT 'CUBE';
SELECT sum(e), sum(c), count() FROM (SELECT (s LIKE '%1%') OR (s LIKE '%2%') OR (s LIKE '%3%') OR (s LIKE '%4%') AS e, count() AS c FROM (SELECT toString(number % 6) AS s FROM numbers(12)) GROUP BY CUBE(s LIKE '%1%', s LIKE '%2%', s LIKE '%3%', s LIKE '%4%'));
SELECT 'GROUPING SETS';
SELECT * FROM (SELECT (s LIKE '%1%') OR (s LIKE '%2%') OR (s LIKE '%3%') OR (s LIKE '%4%') AS e, count() AS c FROM (SELECT toString(number % 6) AS s FROM numbers(12)) GROUP BY GROUPING SETS ((s LIKE '%1%', s LIKE '%2%'), (s LIKE '%3%', s LIKE '%4%'))) ORDER BY ALL;
SELECT * FROM (SELECT (s LIKE '%1%') OR (s LIKE '%2%') OR (s LIKE '%3%') OR (s LIKE '%4%') AS e, count() AS c FROM (SELECT toString(number % 6) AS s FROM numbers(12)) GROUP BY GROUPING SETS ((s LIKE '%1%', s LIKE '%2%'), (s LIKE '%3%', s LIKE '%4%'))) ORDER BY ALL SETTINGS group_by_use_nulls = 1;
SELECT 'ROLLUP, rolled-up key with its column';
SELECT countIf(e), count() FROM (SELECT s, (s LIKE '%1%') OR (s LIKE '%2%') OR (s LIKE '%3%') OR (s LIKE '%4%') AS e FROM (SELECT toString(number % 6) AS s FROM numbers(12)) GROUP BY ROLLUP(s, s LIKE '%1%'));
SELECT 'ROLLUP, group_by_use_nulls';
SELECT countIf(e IS NULL), count() FROM (SELECT s, (s LIKE '%1%') OR (s LIKE '%2%') OR (s LIKE '%3%') OR (s LIKE '%4%') AS e FROM (SELECT toString(number % 6) AS s FROM numbers(12)) GROUP BY ROLLUP(s, s LIKE '%1%', s LIKE '%2%', s LIKE '%3%', s LIKE '%4%')) SETTINGS group_by_use_nulls = 1;
SELECT * FROM (SELECT (s LIKE '%1%') OR (s LIKE '%2%') OR (s LIKE '%3%') OR (s LIKE '%4%') AS e, count() AS c FROM (SELECT toString(number % 6) AS s FROM numbers(12)) GROUP BY ROLLUP(s LIKE '%1%', s LIKE '%2%', s LIKE '%3%', s LIKE '%4%')) ORDER BY ALL SETTINGS group_by_use_nulls = 1;

SELECT 'key whose argument has its own chain';
SELECT * FROM (WITH toString((s LIKE '%a%') OR (s LIKE '%b%') OR (s LIKE '%c%') OR (s LIKE '%d%')) AS x SELECT x, x LIKE '%1%' AS k, count() AS c FROM (SELECT arrayJoin(['a', 'b', 'x']) AS s) GROUP BY ROLLUP(x, x LIKE '%1%') HAVING (x LIKE '%1%') OR (x LIKE '%11%') OR (x LIKE '%12%') OR (x LIKE '%13%')) ORDER BY ALL SETTINGS enable_identifier_resolve_cache = 0;
SELECT * FROM (WITH toString((s LIKE '%a%') OR (s LIKE '%b%') OR (s LIKE '%c%') OR (s LIKE '%d%')) AS x SELECT x, x LIKE '%1%' AS k, count() AS c FROM (SELECT arrayJoin(['a', 'b', 'x']) AS s) GROUP BY ROLLUP(x, x LIKE '%1%') HAVING (x LIKE '%1%') OR (x LIKE '%11%') OR (x LIKE '%12%') OR (x LIKE '%13%')) ORDER BY ALL SETTINGS enable_identifier_resolve_cache = 1;
SELECT * FROM (WITH toString((s LIKE '%a%') OR (s LIKE '%b%') OR (s LIKE '%c%') OR (s LIKE '%d%')) AS x SELECT count() AS c FROM (SELECT arrayJoin(['a', 'b', 'x']) AS s) GROUP BY x LIKE '%1%', x LIKE '%11%', x LIKE '%12%', x LIKE '%13%' HAVING (x LIKE '%1%') OR (x LIKE '%11%') OR (x LIKE '%12%') OR (x LIKE '%13%')) ORDER BY ALL;

SELECT 'key inside the argument of a rewritten chain';
WITH toString((s LIKE '%1%') OR (s LIKE '%2%') OR (s LIKE '%3%') OR (s LIKE '%4%')) AS x SELECT * FROM (SELECT (x LIKE '%1%') OR (x LIKE '%a%') OR (x LIKE '%b%') OR (x LIKE '%c%') AS e, count() AS c FROM (SELECT toString(number % 6) AS s FROM numbers(12)) GROUP BY s LIKE '%1%', s LIKE '%2%', s LIKE '%3%', s LIKE '%4%') ORDER BY ALL;
WITH toString((s LIKE '%1%') OR (s LIKE '%2%') OR (s LIKE '%3%') OR (s LIKE '%4%')) AS x SELECT countIf(e), count() FROM (SELECT s, (x LIKE '%1%') OR (x LIKE '%7%') OR (x LIKE '%8%') OR (x LIKE '%9%') AS e FROM (SELECT toString(number % 6) AS s FROM numbers(12)) GROUP BY ROLLUP(s, s LIKE '%1%'));

SELECT 'still rewritten';
-- The branches that are not keys are still merged.
SELECT countIf(e), count() FROM (SELECT s, (s LIKE '%1%') OR (s LIKE '%2%') OR (s LIKE '%3%') OR (s LIKE '%4%') OR (s LIKE '%5%') AS e FROM (SELECT toString(number % 6) AS s FROM numbers(12)) GROUP BY ROLLUP(s, s LIKE '%1%'));
SELECT count() FROM (EXPLAIN QUERY TREE SELECT (s LIKE '%1%') OR (s LIKE '%2%') OR (s LIKE '%3%') OR (s LIKE '%4%') OR (s LIKE '%5%') FROM (SELECT toString(number % 6) AS s FROM numbers(12)) GROUP BY ROLLUP(s, s LIKE '%1%')) WHERE explain ILIKE '%function_name: multiSearchAny,%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT (s LIKE '%1%') OR (s LIKE '%2%') OR (s LIKE '%3%') OR (s LIKE '%4%') FROM (SELECT toString(number % 6) AS s FROM numbers(12)) GROUP BY s) WHERE explain ILIKE '%function_name: multiSearchAny,%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT (s LIKE '%1%') OR (s LIKE '%2%') OR (s LIKE '%3%') OR (s LIKE '%4%') FROM (SELECT toString(number % 6) AS s FROM numbers(12)) GROUP BY ROLLUP(s, s LIKE '%9%')) WHERE explain ILIKE '%function_name: multiSearchAny,%';
SELECT count() FROM (EXPLAIN QUERY TREE SELECT count() FROM (SELECT toString(number % 6) AS s FROM numbers(12)) WHERE (s LIKE '%1%') OR (s LIKE '%2%') OR (s LIKE '%3%') OR (s LIKE '%4%')) WHERE explain ILIKE '%function_name: multiSearchAny,%';
SELECT count() FROM (EXPLAIN QUERY TREE WITH toString((s LIKE '%1%') OR (s LIKE '%2%') OR (s LIKE '%3%') OR (s LIKE '%4%')) AS x SELECT (x LIKE '%1%') OR (x LIKE '%a%') OR (x LIKE '%b%') OR (x LIKE '%c%') FROM (SELECT toString(number % 6) AS s FROM numbers(12)) GROUP BY s LIKE '%1%', s LIKE '%2%', s LIKE '%3%', s LIKE '%4%') WHERE explain ILIKE '%function_name: multiSearchAny,%';
