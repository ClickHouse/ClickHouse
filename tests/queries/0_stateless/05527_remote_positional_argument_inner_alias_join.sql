-- Tags: shard
-- https://github.com/ClickHouse/ClickHouse/issues/129857

SET enable_positional_arguments = 1;

SELECT 'JOIN USING';
SELECT negate(number + 1 AS a) AS x FROM remote('127.0.0.2', numbers(3)) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) ORDER BY 1;
SELECT negate(number) AS x, count() FROM remote('127.0.0.2', numbers(3)) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY 1 WITH TOTALS ORDER BY 1 LIMIT 1 BY 1;
SELECT negate(number + 1 AS a) AS x FROM remote('127.0.0.2', numbers(3)) AS t1 JOIN numbers(1, 5) AS t2 USING (number) ORDER BY 1;

SELECT 'FULL JOIN USING';
SELECT number, negate(number), count() FROM numbers(3) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY number, negate(number) WITH CUBE ORDER BY 1, 2, 3 LIMIT 1 BY 2;
SELECT number, negate(number) AS m, count() FROM numbers(3) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY number, negate(number) WITH CUBE ORDER BY 1, 2, 3 LIMIT 1 BY 2;
SELECT number, negate(number) AS m, count() FROM numbers(3) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY number, negate(number) WITH CUBE ORDER BY 1, 2, 3 LIMIT 1 BY m;
SELECT number, negate(number), count() FROM remote('127.0.0.2', numbers(3)) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY number, negate(number) WITH CUBE ORDER BY 1, 2, 3 LIMIT 1 BY 2;
SELECT number, negate(number) AS m, count() FROM remote('127.0.0.2', numbers(3)) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY number, negate(number) WITH CUBE ORDER BY 1, 2, 3 LIMIT 1 BY 2;
SELECT number, negate(number) AS m, count() FROM remote('127.0.0.2', numbers(3)) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY number, negate(number) WITH CUBE ORDER BY 1, 2, 3 LIMIT 1 BY m;
SELECT negate(number), count() FROM numbers(3) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY negate(number) WITH TOTALS ORDER BY 1 LIMIT 1 BY 1;
SELECT negate(number) AS m, count() FROM numbers(3) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY negate(number) WITH TOTALS ORDER BY 1 LIMIT 1 BY 1;
SELECT negate(number) AS m, count() FROM numbers(3) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY negate(number) WITH TOTALS ORDER BY 1 LIMIT 1 BY m;
SELECT negate(number), count() FROM remote('127.0.0.2', numbers(3)) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY negate(number) WITH TOTALS ORDER BY 1 LIMIT 1 BY 1;
SELECT negate(number) AS m, count() FROM remote('127.0.0.2', numbers(3)) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY negate(number) WITH TOTALS ORDER BY 1 LIMIT 1 BY 1;
SELECT negate(number) AS m, count() FROM remote('127.0.0.2', numbers(3)) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY negate(number) WITH TOTALS ORDER BY 1 LIMIT 1 BY m;
SELECT number, grouping(number) FROM numbers(3) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY number WITH ROLLUP ORDER BY 1, 2;
SELECT number AS n, grouping(n) FROM numbers(3) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY 1 WITH ROLLUP ORDER BY 1, 2;
SELECT * FROM numbers(3) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY number ORDER BY 1 LIMIT 1 BY 1;
SELECT number, grouping(number) FROM remote('127.0.0.2', numbers(3)) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY number WITH ROLLUP ORDER BY 1, 2;
SELECT number AS n, grouping(n) FROM remote('127.0.0.2', numbers(3)) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY 1 WITH ROLLUP ORDER BY 1, 2;
SELECT * FROM remote('127.0.0.2', numbers(3)) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY number ORDER BY 1 LIMIT 1 BY 1;
SELECT number, grouping(number) AS g FROM numbers(3) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY CUBE(number) ORDER BY 2, 1;
SELECT number, grouping(number) AS g FROM remote('127.0.0.2', numbers(3)) AS t1 FULL JOIN numbers(1, 3) AS t2 USING (number) GROUP BY CUBE(number) ORDER BY 2, 1;
