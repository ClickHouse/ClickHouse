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

SELECT 'expressions with other constants';
SELECT intDiv(number, 24 * 60 * 60) AS x, count() FROM numbers(86400, 2) GROUP BY intDiv(number, 86400);
SELECT intDiv(number, 24 * 60 * 60) + 1 AS x, count() FROM remote('127.0.0.{2,3}', numbers(86400, 2)) GROUP BY intDiv(number, 86400) HAVING intDiv(number, toUInt32(86400)) = 1;
SELECT intDiv(number, 4) AS x, count() FROM numbers(8) GROUP BY x ORDER BY negate(intDiv(number, toUInt8(4)));
SELECT intDiv(number, 4) AS x, count() FROM numbers(8) GROUP BY 1 ORDER BY 1 LIMIT 5 BY x, length(toString(intDiv(number, toUInt8(4))));
SELECT intDiv(number, 4) AS x, sum(count()) OVER (ORDER BY intDiv(number, toUInt8(4))) AS s FROM numbers(8) GROUP BY x ORDER BY x;
SELECT intDiv(number, 4) AS x, intDiv(number, toUInt8(4)) AS y, count() FROM numbers(8) GROUP BY x, y WITH ROLLUP ORDER BY ALL;
SELECT intDiv(number, 4) AS x, intDiv(number, toUInt8(4)) AS y, max(intDiv(number, intDiv(8, 2))) AS m FROM numbers(8) GROUP BY GROUPING SETS ((x), (y)) ORDER BY ALL;
SELECT intDiv(number, 4) + 1 AS x, intDiv(number, toUInt8(4)) + 1 AS y, count() AS c FROM numbers(10) GROUP BY GROUPING SETS ((x), (y), (intDiv(number, 4))) HAVING intDiv(number, toUInt8(4)) + 1 > 0 ORDER BY ALL;
SELECT intDiv(number, intDiv(8, 2)) AS x, count() AS c FROM numbers(10) GROUP BY intDiv(number, toUInt8(4)), x WITH ROLLUP ORDER BY x, intDiv(number, 4), c;
SELECT intDiv(number, 4) AS x, intDiv(number, toUInt8(4)) AS y, count() AS c FROM numbers(10) GROUP BY GROUPING SETS ((x), (y)) ORDER BY x, y, c LIMIT 1 BY x, y;
SELECT intDiv(number, 4) AS x, intDiv(number, toUInt8(4)) AS y, count() AS c FROM numbers(10) GROUP BY GROUPING SETS ((x), (y)) ORDER BY x, y, c LIMIT 1 BY x, bitAnd(y, 7);
SELECT intDiv(number, toUInt8(4)) AS x, count() AS c FROM numbers(10) GROUP BY x, intDiv(number, intDiv(8, 2)) WITH ROLLUP ORDER BY x, c LIMIT 1 BY x, bitAnd(intDiv(number, 4), 7);
SELECT max(intDiv(number, 4)) AS m, count() AS c FROM numbers(10) GROUP BY intDiv(number, toUInt8(4)), intDiv(number, intDiv(8, 2)) ORDER BY ALL;
SELECT arrayJoin([number, 2]) AS x, toString(x) AS y, count() AS c FROM numbers(3) GROUP BY x, toString(arrayJoin([number, toUInt8(2)])) WITH TOTALS ORDER BY x, y, c;
SELECT arrayJoin([number, 2]) AS x, toString(x) AS y, count() AS c FROM numbers(3) GROUP BY x, toString(arrayJoin([number, toUInt8(2)])) ORDER BY x, y, c SETTINGS max_block_size = 1;
SELECT arrayJoin([number, 2]) AS x, arrayJoin([number, toUInt8(2)]) AS y, count() AS c FROM numbers(3) GROUP BY x, y ORDER BY x, c, y SETTINGS max_block_size = 1;
SELECT rand64(number + 2.) % 2 AS x, rand64(number + toFloat64(2)) % 2 AS y FROM numbers(1000) GROUP BY 1, 2 ORDER BY 1, 2 DESC LIMIT 1 BY x;
SELECT rand64(number + 2.) % 2 AS x, rand64(number + toFloat64(2)) % 2 AS y FROM numbers(1000) GROUP BY x, y ORDER BY x, y DESC LIMIT 1 BY x;
SELECT arrayMap(a -> rand(a) % 2, [number]) AS x, arrayMap(a -> rand(a) % toUInt8(2), [number]) AS y FROM numbers(1000) GROUP BY 1, 2 ORDER BY 1, 2 DESC LIMIT 1 BY x;
SELECT arrayJoin([1, 2]) + 2 AS x, count() AS c FROM numbers(3) GROUP BY x ORDER BY x, arrayJoin([1, 2]) + toUInt8(2);
SELECT arrayJoin([1, 2]) + 2 AS x, max(arrayJoin([1, 2])) AS m FROM numbers(3) GROUP BY x ORDER BY x LIMIT 10 BY x, arrayJoin([1, 2]) + toUInt8(2);
SELECT arrayJoin([number, 2]) AS x, arrayJoin([3, 4]) AS y FROM numbers(3) GROUP BY x ORDER BY x, arrayJoin([number, toUInt8(2)]) - y;
SELECT intDiv(a.number, 4) AS x, intDiv(b.number, toUInt8(4)) AS y, count() AS c FROM numbers(8) AS a CROSS JOIN numbers(8) AS b GROUP BY intDiv(a.number, 4), intDiv(b.number, 4) ORDER BY x, y;
SELECT intDiv(a.number, 4) AS x, intDiv(b.number, toUInt8(4)) AS y, max(intDiv(b.number, intDiv(8, 2))) AS m FROM numbers(8) AS a CROSS JOIN numbers(8) AS b GROUP BY GROUPING SETS ((intDiv(a.number, 4), intDiv(b.number, 4)), (intDiv(a.number, 4))) ORDER BY x, y, m;
SELECT a.number + 1 AS x, b.number + 1 AS y, count() AS c FROM remote('127.0.0.{2,3}', numbers(2)) AS a CROSS JOIN numbers(2) AS b GROUP BY a.number + 1, b.number + 1 HAVING b.number + toUInt8(1) = 2 ORDER BY x, y;
SELECT number AS x, arrayMap(a -> a + toUInt8(2), [x]) AS y FROM numbers(2) GROUP BY x, arrayMap(a -> a + 2, [x]) WITH ROLLUP ORDER BY ALL;
SELECT arrayMap(a -> a + toUInt8(2), [number]) AS x, count() AS c FROM numbers(2) GROUP BY arrayMap(a -> a + 2, [number]) ORDER BY ALL;
DROP TABLE IF EXISTS t;
CREATE TABLE t (x UInt8, y UInt8 ALIAS intDiv(x, toUInt8(4))) ENGINE = Memory;
INSERT INTO t (x) SELECT number FROM numbers(8);
SELECT y, count() FROM t GROUP BY y, intDiv(x, 4) ORDER BY ALL;
DROP TABLE t;
SELECT intDiv(number, 4) AS x, max(intDiv(number, toUInt8(4))) AS m, count() AS c FROM numbers(4, 3) GROUP BY GROUPING SETS ((x), (x, number)) ORDER BY ALL;
SELECT intDiv(number, 4) AS x, count() AS c, intDiv(number, toUInt8(4)) AS y FROM numbers(8) GROUP BY x, number WITH ROLLUP ORDER BY x, y, c;
SELECT rand() % 4 AS x, max(rand() % toUInt8(4)) AS m, toString(rand() % toUInt8(4)) AS y FROM numbers(1000) GROUP BY x, y HAVING x != m OR toString(x) != y;
SELECT max(formatDateTime(toDateTime(0, 'Asia/Tokyo') + number, '%H')) AS m FROM numbers(3) GROUP BY formatDateTime(toDateTime(0, 'UTC') + number, '%H');
SELECT number AS x, count() FROM numbers(3) GROUP BY x, toUInt8(2), intDiv(2, x) ORDER BY x;
SELECT intDiv(number, 4) AS x, count() AS c, intDiv(number, toUInt8(4)) AS y FROM numbers(8) GROUP BY x, number WITH ROLLUP ORDER BY x + c * 0, y + c * 0, c;
SELECT arrayJoin([x, 2]) AS a, arrayJoin([x, toUInt8(2)]) AS b FROM (SELECT materialize(toUInt8(5)) AS x) ORDER BY a, b;
SELECT arrayJoin([x, 2]) AS a FROM (SELECT materialize(toUInt8(5)) AS x) ORDER BY a, arrayJoin([x, toUInt8(2)]);
SELECT number % 3 AS x, x + 1 AS y, sum(number) AS s FROM numbers(6) GROUP BY x, y WITH TOTALS ORDER BY x WITH FILL FROM 0 TO 6 INTERPOLATE (s AS x + toUInt8(1));
SELECT intDiv(number, 4) AS x, max(intDiv(number, toUInt8(4))) AS m, count() FROM numbers(8) GROUP BY x WITH CUBE HAVING intDiv(number, toUInt8(4)) < 1 ORDER BY ALL;

SELECT 'grouping sets';
SELECT number % 2 + 1 AS a, intDiv(number, 2) AS b, a + b AS c, any(a) + b AS d FROM numbers(4) GROUP BY GROUPING SETS ((1, 2, 3), (1, 3)) ORDER BY ALL;
SELECT number % 2 + 1 AS a, intDiv(number, 2) AS b, a + b AS c, any(a) + b AS d FROM numbers(4) GROUP BY GROUPING SETS ((a, b, c), (a, c)) ORDER BY ALL;

SELECT 'grouping';
SELECT number + 1 AS a, toString(a) AS b, grouping(b) AS g FROM remote('127.0.0.{2,3}', numbers(2)) GROUP BY 1, 2 ORDER BY 1;
SELECT number + 1 AS a, toString(a) AS b, grouping(b) AS g FROM remote('127.0.0.{2,3}', numbers(2)) GROUP BY a, b ORDER BY a;
SELECT number + 1 AS a, toString(a) AS b, count() FROM remote('127.0.0.{2,3}', numbers(2)) GROUP BY 1, 2 HAVING grouping(b) = 0 ORDER BY 1;
SELECT number AS a, ((number, 2), 2) AS b, grouping(a) AS g FROM remote('127.0.0.{2,3}', numbers(2)) GROUP BY a, b ORDER BY a;
