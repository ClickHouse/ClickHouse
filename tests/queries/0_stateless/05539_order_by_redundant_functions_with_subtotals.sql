-- Tags: shard
-- A `GROUP BY` key that is a function of another key is not one in subtotals, where it can have a default value.

SET optimize_redundant_functions_in_order_by = 1;

SELECT x AS s, lower(x) AS l, lower(max(lower(x))) AS m FROM values('x String', 'b', 'A', 'a', 'C') GROUP BY GROUPING SETS ((l), (l, s)) ORDER BY s, m;
SELECT x AS s, lower(x) AS l, lower(max(lower(x))) AS m FROM remote('127.0.0.{2,3}', values('x String', 'b', 'A', 'a', 'C')) GROUP BY GROUPING SETS ((l), (l, s)) ORDER BY s, m;
SELECT number AS n, negate(number) AS m, count() AS c FROM numbers(3) GROUP BY GROUPING SETS ((n, m), (n)) ORDER BY n, m, c;
SELECT number AS n, negate(number) AS m, count() AS c FROM numbers(3) GROUP BY ROLLUP(n, m) ORDER BY n, m DESC, c;
SELECT number AS n, negate(number) AS m, count() AS c FROM numbers(3) GROUP BY CUBE(n, m) ORDER BY n, m DESC, c;

SELECT 'positional arguments with aliases';
SET enable_positional_arguments = 1;
SELECT number AS n, negate(abs(number) AS a) AS m FROM remote('127.0.0.2', numbers(2)) GROUP BY n WITH ROLLUP ORDER BY n, 2;
SELECT (SELECT groupArray(k) FROM (SELECT number AS n, negate(abs(number) AS a) AS m, bitNot(m) AS k FROM remote('127.0.0.2', numbers(2)) GROUP BY n WITH ROLLUP ORDER BY n, 2, 3 LIMIT 3));
SELECT 'self join';
SELECT count() FROM (SELECT b.number, negate(a.number) AS x FROM numbers(3) AS a CROSS JOIN numbers(3) AS b GROUP BY b.number, a.number WITH ROLLUP ORDER BY b.number DESC, negate(b.number) LIMIT 1 WITH TIES) WHERE x != 5;
