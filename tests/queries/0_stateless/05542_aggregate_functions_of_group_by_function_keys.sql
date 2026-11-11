-- Tags: shard
-- Aggregate functions of `GROUP BY` keys, and functions of keys, that cannot be replaced with the key, or calculated from it.

SET enable_positional_arguments = 1;
SET optimize_aggregators_of_group_by_keys = 1, optimize_group_by_function_keys = 1;

SELECT 'types with a custom name';
SELECT (number % 2)::Bool AS k, groupBitOr(k) AS o, toTypeName(o) FROM numbers(4) GROUP BY 1 ORDER BY 1;
SELECT (number % 2)::Bool AS k, groupBitOr(k) AS o, toTypeName(o) FROM numbers(4) GROUP BY k ORDER BY k;
SELECT number % 2 = 1 AS k, max(k) AS m, toTypeName(m) FROM numbers(4) GROUP BY 1 ORDER BY 1;

SELECT 'arrayJoin';
SELECT [number, number + 5] AS k, arrayJoin(k) AS x, arrayJoin(max(k)) AS y FROM numbers(1) GROUP BY 1 ORDER BY ALL;
SELECT [number, number + 5] AS k, arrayJoin(k) AS x, arrayJoin(max(k)) AS y FROM numbers(1) GROUP BY k ORDER BY ALL;
SELECT [number, number + 5] AS k, arrayJoin(k) AS x FROM numbers(1) GROUP BY 1 HAVING arrayJoin([max(k)[1], 5]) = 0 ORDER BY ALL;

SELECT 'constants';
WITH 24 * 60 * 60 AS day SELECT intDiv(number, 86400) AS x, intDiv(number, day) * day AS y, max(intDiv(number, day)) AS m, count() FROM numbers(86400, 2) GROUP BY 1, 2;
WITH 24 * 60 * 60 AS day SELECT intDiv(number, 86400) AS x, intDiv(number, day) * day AS y, max(intDiv(number, day)) AS m, count() FROM numbers(86400, 2) GROUP BY x, y;
WITH 24 * 60 * 60 AS day SELECT intDiv(number, 86400) AS x, intDiv(number, day) * day AS y, max(intDiv(number, day)) AS m, count() FROM remote('127.0.0.{2,3}', numbers(86400, 2)) GROUP BY 1, 2;
WITH 24 * 60 * 60 AS day SELECT intDiv(number, 86400) AS x, max(x) AS m, max(intDiv(number, day)) AS n, count() FROM remote('127.0.0.2', numbers(86400, 2)) GROUP BY 1;

SELECT 'grouping sets';
SELECT number % 2 + 1 AS a, intDiv(number, 2) AS b, a + b AS c, any(a) + b AS d FROM numbers(4) GROUP BY GROUPING SETS ((1, 2, 3), (1, 3)) ORDER BY ALL;
SELECT number % 2 + 1 AS a, intDiv(number, 2) AS b, a + b AS c, any(a) + b AS d FROM numbers(4) GROUP BY GROUPING SETS ((a, b, c), (a, c)) ORDER BY ALL;

SELECT 'grouping';
SELECT number + 1 AS a, toString(a) AS b, grouping(b) AS g FROM remote('127.0.0.{2,3}', numbers(2)) GROUP BY 1, 2 ORDER BY 1;
SELECT number + 1 AS a, toString(a) AS b, grouping(b) AS g FROM remote('127.0.0.{2,3}', numbers(2)) GROUP BY a, b ORDER BY a;
SELECT number + 1 AS a, toString(a) AS b, count() FROM remote('127.0.0.{2,3}', numbers(2)) GROUP BY 1, 2 HAVING grouping(b) = 0 ORDER BY 1;
