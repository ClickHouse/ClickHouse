-- Tags: shard
-- https://github.com/ClickHouse/ClickHouse/issues/129857

SET enable_positional_arguments = 1;

SELECT 'GROUP BY';
SELECT intDiv(number + 1 AS a, 2) AS x, sum(number) FROM remote('127.0.0.{2,3}', numbers(5)) GROUP BY 1 ORDER BY x;
SELECT intDiv(number + 1 AS a, 2) AS x, sum(number) FROM remote('127.0.0.2', numbers(5)) GROUP BY 1 WITH ROLLUP ORDER BY 1, 2;
SELECT intDiv(number + 1 AS a, 2) AS x, a % 2 AS y, sum(number) FROM remote('127.0.0.2', numbers(3)) GROUP BY 1, 2 WITH CUBE ORDER BY 1, 2, 3;
SELECT negate(number % 2 + 1 AS a) AS x, sum(number) AS s FROM remote('127.0.0.2', numbers(5)) GROUP BY GROUPING SETS ((1), ()) ORDER BY x, s;
SELECT negate(number % 2 + 1 AS a) AS x, count() FROM remote('127.0.0.{2,3}', numbers(4)) GROUP BY 1 WITH TOTALS ORDER BY 1;
SELECT negate(number + 1 AS a) AS x, sum(number) AS s FROM remote('127.0.0.2', numbers(2)) GROUP BY 1 ORDER BY 1 SETTINGS group_by_use_nulls = 1;
SELECT number % 2 AS y, toString(number % 2) AS x, max(x) AS m FROM remote('127.0.0.2', numbers(4)) GROUP BY GROUPING SETS ((1, 2), (1)) ORDER BY ALL;
SELECT number % 3 AS m, grouping(m) AS g FROM numbers(9) GROUP BY ROLLUP(m) ORDER BY 2, 1;
SELECT number % 3 AS m, grouping(m) AS g FROM remote('127.0.0.{2,3}', numbers(9)) GROUP BY ROLLUP(m) ORDER BY 2, 1;
SELECT number % 3 AS m, grouping(m) AS g FROM remote('127.0.0.{2,3}', numbers(9)) GROUP BY GROUPING SETS ((m), ()) ORDER BY 2, 1;
SELECT number % 3 AS m, grouping(m) AS g FROM remote('127.0.0.{2,3}', numbers(9)) GROUP BY GROUPING SETS ((1), ()) ORDER BY 2, 1;
SELECT number % 3 AS m, grouping(m) AS g FROM remote('127.0.0.{2,3}', numbers(9)) GROUP BY 1 WITH CUBE ORDER BY g, m;

SELECT 'arithmetic in aggregate functions';
SELECT number * 2 AS a, max(a) AS m FROM remote('127.0.0.{2,3}', numbers(3)) GROUP BY 1 ORDER BY 2;
SELECT max((number * 2) AS a), min(a) AS m FROM remote('127.0.0.{2,3}', numbers(3)) ORDER BY 1, 2;
SELECT number % 2 AS k, sum(number * 7) AS s FROM remote('127.0.0.{2,3}', numbers(3)) GROUP BY 1 ORDER BY 2;
SELECT number % 2 AS k, min(1 - number) AS s FROM remote('127.0.0.{2,3}', numbers(3)) GROUP BY 1 ORDER BY 2;
WITH number / 2 AS h SELECT number % 2 AS k, avg(h) AS s, max(h) AS m FROM remote('127.0.0.{2,3}', numbers(3)) GROUP BY 1 ORDER BY 2, 3;
