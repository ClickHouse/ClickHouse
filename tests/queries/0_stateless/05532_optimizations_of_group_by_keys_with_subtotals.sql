-- Tags: shard
-- A `GROUP BY` key that is a function of another key is not one in subtotals, where it can have a default value.

SET enable_positional_arguments = 1;
SET optimize_aggregators_of_group_by_keys = 1;
SET optimize_limit_by_function_keys = 1;

SELECT 'aggregate functions';
SELECT number % 2 AS y, toString(number % 2) AS x, max(x) AS m FROM numbers(4) GROUP BY GROUPING SETS ((1, 2), (1)) ORDER BY ALL;
SELECT number % 2 AS y, toString(number % 2) AS x, max(concat(x, '!')) AS m, max(y + 1) AS n FROM numbers(4) GROUP BY GROUPING SETS ((1, 2), (1)) ORDER BY ALL;
SELECT number % 2 AS y, toString(number % 2) AS x FROM numbers(4) GROUP BY GROUPING SETS ((1, 2), (1)) HAVING max(toString(number % 2)) = '1' ORDER BY ALL;
SELECT number % 2 AS y, toString(number % 2) AS x, max(x) AS m FROM numbers(4) GROUP BY GROUPING SETS ((y, x), (y)) ORDER BY ALL;
SELECT number % 2 AS y, toString(number % 2) AS x, max(concat(x, '!')) AS m, max(y + 1) AS n FROM numbers(4) GROUP BY GROUPING SETS ((y, x), (y)) ORDER BY ALL;
SELECT number % 2 AS y, toString(number % 2) AS x FROM numbers(4) GROUP BY GROUPING SETS ((y, x), (y)) HAVING max(toString(number % 2)) = '1' ORDER BY ALL;
SELECT number % 2 AS y, toString(number % 2) AS x, max(x) AS m FROM remote('127.0.0.{2,3}', numbers(4)) GROUP BY GROUPING SETS ((1, 2), (1)) ORDER BY ALL;
SELECT number % 2 AS y, toString(number % 2) AS x, max(concat(x, '!')) AS m, max(y + 1) AS n FROM remote('127.0.0.{2,3}', numbers(4)) GROUP BY GROUPING SETS ((1, 2), (1)) ORDER BY ALL;
SELECT number % 2 AS y, toString(number % 2) AS x FROM remote('127.0.0.{2,3}', numbers(4)) GROUP BY GROUPING SETS ((1, 2), (1)) HAVING max(toString(number % 2)) = '1' ORDER BY ALL;
SELECT number % 2 AS y, toString(number % 2) AS x, max(x) AS m FROM remote('127.0.0.{2,3}', numbers(4)) GROUP BY GROUPING SETS ((y, x), (y)) ORDER BY ALL;
SELECT number % 2 AS y, toString(number % 2) AS x, max(concat(x, '!')) AS m, max(y + 1) AS n FROM remote('127.0.0.{2,3}', numbers(4)) GROUP BY GROUPING SETS ((y, x), (y)) ORDER BY ALL;
SELECT number % 2 AS y, toString(number % 2) AS x FROM remote('127.0.0.{2,3}', numbers(4)) GROUP BY GROUPING SETS ((y, x), (y)) HAVING max(toString(number % 2)) = '1' ORDER BY ALL;

SELECT 'not deterministic';
SELECT [number, number + 1] AS a, min(arrayJoin(a)) AS m FROM numbers(2) GROUP BY 1 ORDER BY ALL;
SELECT [number, number + 1] AS a, min(arrayJoin(a)) AS m FROM numbers(2) GROUP BY a ORDER BY ALL;
SELECT [number, number + 1] AS a, min(arrayJoin(a)) AS m FROM remote('127.0.0.{2,3}', numbers(2)) GROUP BY 1 ORDER BY ALL;
SELECT [number, number + 1] AS a, min(arrayJoin(a)) AS m FROM remote('127.0.0.{2,3}', numbers(2)) GROUP BY a ORDER BY ALL;
SELECT number % 3 AS a FROM numbers(6) GROUP BY 1 HAVING max(arrayJoin([a, 7])) = 7 ORDER BY ALL;
SELECT number % 3 AS a FROM numbers(6) GROUP BY a HAVING max(arrayJoin([a, 7])) = 7 ORDER BY ALL;
SELECT number % 3 AS a, min(rand64(a)) < 922337203685477580 AS m FROM numbers(3000) GROUP BY 1 ORDER BY ALL;
SELECT number % 3 AS a, min(rand64(a)) < 922337203685477580 AS m FROM numbers(3000) GROUP BY a ORDER BY ALL;

SELECT 'WITH FILL';
SELECT negate(number % 3) AS x1, max(x1) AS x2 FROM numbers(9) GROUP BY 1 ORDER BY 2 WITH FILL FROM -2 TO 3;
SELECT negate(number % 3) AS x1, max(x1) AS x2 FROM numbers(9) GROUP BY x1 ORDER BY x2 WITH FILL FROM -2 TO 3;

SELECT 'LIMIT BY';
SELECT (number % 2 AS a) + 1 AS x0, number % 2 AS x1, sum(number) FROM numbers(6) GROUP BY 1, 2 WITH ROLLUP ORDER BY 1, 2, 3 LIMIT 1 BY 1, 2;
SELECT (number % 2 AS a) + 1 AS x0, number % 2 AS x1, sum(number) FROM numbers(6) GROUP BY 1, 2 WITH ROLLUP ORDER BY 1, 2, 3 LIMIT 1 BY x0, x1;
SELECT (number % 2 AS a) + 1 AS x0, number % 2 AS x1, sum(number) FROM numbers(6) GROUP BY 1, 2 WITH CUBE ORDER BY 1, 2, 3 LIMIT 1 BY 1, 2;
SELECT (number % 2 AS a) + 1 AS x0, number % 2 AS x1, sum(number) FROM numbers(6) GROUP BY 1, 2 WITH CUBE ORDER BY 1, 2, 3 LIMIT 1 BY x0, x1;
SELECT (number % 2 AS a) + 1 AS x0, number % 2 AS x1, sum(number) FROM numbers(6) GROUP BY GROUPING SETS ((1, 2), (1), (2)) ORDER BY 1, 2, 3 LIMIT 1 BY 1, 2;
SELECT (number % 2 AS a) + 1 AS x0, number % 2 AS x1, sum(number) FROM numbers(6) GROUP BY GROUPING SETS ((1, 2), (1), (2)) ORDER BY 1, 2, 3 LIMIT 1 BY x0, x1;
SELECT (number % 2 AS a) + 1 AS x0, number % 2 AS x1, sum(number) FROM remote('127.0.0.2', numbers(6)) GROUP BY 1, 2 WITH ROLLUP ORDER BY 1, 2, 3 LIMIT 1 BY 1, 2;
SELECT (number % 2 AS a) + 1 AS x0, number % 2 AS x1, sum(number) FROM remote('127.0.0.2', numbers(6)) GROUP BY 1, 2 WITH ROLLUP ORDER BY 1, 2, 3 LIMIT 1 BY x0, x1;
SELECT (number % 2 AS a) + 1 AS x0, number % 2 AS x1, sum(number) FROM remote('127.0.0.2', numbers(6)) GROUP BY 1, 2 WITH CUBE ORDER BY 1, 2, 3 LIMIT 1 BY 1, 2;
SELECT (number % 2 AS a) + 1 AS x0, number % 2 AS x1, sum(number) FROM remote('127.0.0.2', numbers(6)) GROUP BY 1, 2 WITH CUBE ORDER BY 1, 2, 3 LIMIT 1 BY x0, x1;
SELECT (number % 2 AS a) + 1 AS x0, number % 2 AS x1, sum(number) FROM remote('127.0.0.2', numbers(6)) GROUP BY GROUPING SETS ((1, 2), (1), (2)) ORDER BY 1, 2, 3 LIMIT 1 BY 1, 2;
SELECT (number % 2 AS a) + 1 AS x0, number % 2 AS x1, sum(number) FROM remote('127.0.0.2', numbers(6)) GROUP BY GROUPING SETS ((1, 2), (1), (2)) ORDER BY 1, 2, 3 LIMIT 1 BY x0, x1;
SELECT number % 2 AS x1, number % 2 + 1 AS x0, x0 * 2 AS z, count() FROM numbers(6) GROUP BY x1, x0 WITH ROLLUP ORDER BY 1, 2, 3, 4 LIMIT 1 BY x1, z;
SELECT (number % 2) + 1 AS x0, number % 2 AS x1, sum(number) AS s FROM numbers(6) GROUP BY 1, 2 WITH ROLLUP ORDER BY 1, 2, 3 LIMIT 1 BY ALL;
SELECT (number % 2) + 1 AS x0, number % 2 AS x1, sum(number) AS s FROM numbers(6) GROUP BY 1, 2 WITH ROLLUP ORDER BY 1 NULLS FIRST, 2 NULLS FIRST, 3 LIMIT 1 BY 1, 2 SETTINGS group_by_use_nulls = 1;
