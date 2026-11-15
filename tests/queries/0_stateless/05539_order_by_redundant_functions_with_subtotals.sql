-- Tags: shard
-- A `GROUP BY` key that is a function of another key is not one in subtotals, where it can have a default value.

SET optimize_redundant_functions_in_order_by = 1;

SELECT x AS s, lower(x) AS l, lower(max(lower(x))) AS m FROM values('x String', 'b', 'A', 'a', 'C') GROUP BY GROUPING SETS ((l), (l, s)) ORDER BY s, m;
SELECT x AS s, lower(x) AS l, lower(max(lower(x))) AS m FROM remote('127.0.0.{2,3}', values('x String', 'b', 'A', 'a', 'C')) GROUP BY GROUPING SETS ((l), (l, s)) ORDER BY s, m;
SELECT number AS n, negate(number) AS m, count() AS c FROM numbers(3) GROUP BY GROUPING SETS ((n, m), (n)) ORDER BY n, m, c;
SELECT number AS n, negate(number) AS m, count() AS c FROM numbers(3) GROUP BY ROLLUP(n, m) ORDER BY n, m DESC, c;
SELECT number AS n, negate(number) AS m, count() AS c FROM numbers(3) GROUP BY CUBE(n, m) ORDER BY n, m DESC, c;
