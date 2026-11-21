-- Tags: shard
-- https://github.com/ClickHouse/ClickHouse/issues/129857

SET enable_positional_arguments = 1;

SELECT 'ORDER BY';
SELECT negate(number + 1 AS a) AS x FROM remote('127.0.0.2', numbers(3)) ORDER BY 1;
SELECT negate(number + 1 AS a) AS x FROM remote('127.0.0.1', numbers(3)) ORDER BY 1;
SELECT DISTINCT negate(number + 1 AS a) AS x FROM remote('127.0.0.{2,3}', numbers(3)) ORDER BY 1 DESC;
SELECT negate(number + 1 AS a) AS x, a AS y FROM remote('127.0.0.2', numbers(3)) WHERE a > 1 ORDER BY -2, -1;
WITH 5 AS n SELECT number + n AS x FROM remote('127.0.0.{2,3}', numbers(2)) ORDER BY 1;
SELECT intDiv(count() AS c, 2) AS x FROM remote('127.0.0.{2,3}', numbers(3)) GROUP BY number ORDER BY 1;
SELECT sum(number * 7) AS x FROM remote('127.0.0.{2,3}', numbers(3)) ORDER BY 1;
SELECT negate(length(hostName()) * 0 + number + 1 AS a) AS x FROM remote('127.0.0.2', numbers(2)) ORDER BY 1;
SELECT negate(number + 1 AS number) AS x FROM remote('127.0.0.2', numbers(2)) ORDER BY 1 SETTINGS prefer_column_name_to_alias = 1;

SELECT 'repeated and nested aliases';
SELECT negate(number + 1 AS a) AS x, abs(number + 1 AS a) AS y FROM remote('127.0.0.2', numbers(2)) ORDER BY 1, 2;
SELECT negate(number + 1 AS a) AS x, negate(number + 1 AS a) AS x FROM remote('127.0.0.2', numbers(2)) ORDER BY 1, 2;
SELECT number + (1 AS one) AS a, negate(a) FROM remote('127.0.0.2', numbers(2)) ORDER BY 2;
SELECT number + (1 AS one) AS a, negate(a AS b) AS x, a + x * 3 AS y FROM remote('127.0.0.2', numbers(3)) ORDER BY 3, 2, 1;

SELECT 'LIMIT BY';
SELECT intDiv(number + 1 AS a, 2) AS x FROM remote('127.0.0.2', numbers(5)) ORDER BY x LIMIT 1 BY 1;
SELECT negate(number) AS a, negate(a) AS b FROM numbers(3) GROUP BY CUBE(1) ORDER BY 1 LIMIT 1 BY 2;
SELECT negate(number % 3) AS a, negate(a) AS b FROM numbers(6) GROUP BY a WITH TOTALS ORDER BY 1 LIMIT 1 BY 2;
SELECT negate(number % 3) AS a, negate(a) AS b FROM remote('127.0.0.{2,3}', numbers(6)) GROUP BY 1 WITH TOTALS ORDER BY 1 LIMIT 1 BY 2;

SELECT 'window functions';
SELECT negate(number + 1 AS a) AS x, sum(x) OVER (ORDER BY number) AS s FROM remote('127.0.0.2', numbers(3)) ORDER BY -1, -2;
SELECT row_number() OVER () + (1 AS one) AS x FROM remote('127.0.0.2', numbers(2)) ORDER BY 1 DESC;

SELECT 'serialized query plan';
SELECT negate(number + 1 AS a) AS x FROM remote('127.0.0.2', numbers(2)) ORDER BY 1 SETTINGS serialize_query_plan = 1;
