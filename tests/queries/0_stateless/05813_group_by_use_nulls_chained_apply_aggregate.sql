-- `* APPLY f APPLY <aggregate>` builds `<aggregate>(f(x))`: the transformers before the aggregate one see GROUP BY keys
-- before aggregation, like the inline form. Each chained query is followed by its inline twin.

SET group_by_use_nulls = 1;

SELECT * APPLY abs APPLY sum FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;
SELECT sum(abs(number)) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;
SELECT * APPLY abs APPLY sum FROM numbers(3) GROUP BY number WITH CUBE ORDER BY 1;
SELECT sum(abs(number)) FROM numbers(3) GROUP BY number WITH CUBE ORDER BY 1;
SELECT * APPLY abs APPLY sum FROM numbers(3) GROUP BY GROUPING SETS ((number), ()) ORDER BY 1;
SELECT sum(abs(number)) FROM numbers(3) GROUP BY GROUPING SETS ((number), ()) ORDER BY 1;
SELECT * APPLY abs APPLY sum FROM numbers(3) GROUP BY number WITH ROLLUP WITH TOTALS ORDER BY 1;
SELECT sum(abs(number)) FROM numbers(3) GROUP BY number WITH ROLLUP WITH TOTALS ORDER BY 1;

SELECT * APPLY (x -> x) APPLY sum FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;
SELECT sum(number) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;
SELECT * APPLY (x -> x + 1) APPLY sum FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;
SELECT sum(number + 1) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;

SELECT * APPLY abs APPLY (x -> sum(x)) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;
SELECT sum(abs(number)) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;
SELECT * APPLY abs APPLY (x -> toString(sum(x))) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;
SELECT toString(sum(abs(number))) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;
SELECT * APPLY abs APPLY (x -> sum(x) + max(x)) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;
SELECT sum(abs(number)) + max(abs(number)) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;
SELECT * APPLY abs APPLY quantileExact(0.5) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;
SELECT quantileExact(0.5)(abs(number)) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;

SELECT * APPLY (x -> x + 1) APPLY abs APPLY sum FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;
SELECT sum(abs(number + 1)) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;
SELECT * APPLY abs APPLY sum APPLY toString FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;
SELECT toString(sum(abs(number))) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;

SELECT * APPLY abs APPLY (x -> any(toTypeName(x))) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;
SELECT any(toTypeName(abs(number))) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;
SELECT * APPLY abs APPLY toTypeName FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;
SELECT toTypeName(abs(number)) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;

SELECT COLUMNS('a') APPLY (x -> x + b) APPLY sum FROM (SELECT number AS a, number * 2 AS b FROM numbers(3)) GROUP BY a, b WITH ROLLUP ORDER BY 1;
SELECT sum(a + b) FROM (SELECT number AS a, number * 2 AS b FROM numbers(3)) GROUP BY a, b WITH ROLLUP ORDER BY 1;
SELECT * APPLY abs APPLY sum FROM (SELECT number AS a, number * 2 AS b FROM numbers(3)) GROUP BY a, b WITH ROLLUP ORDER BY a, b;
SELECT sum(abs(a)), sum(abs(b)) FROM (SELECT number AS a, number * 2 AS b FROM numbers(3)) GROUP BY a, b WITH ROLLUP ORDER BY a, b;

SELECT * APPLY abs APPLY (x -> sum(x) OVER ()) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;
SELECT sum(abs(number)) OVER () FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;
SELECT * APPLY abs APPLY (x -> sum(sum(x)) OVER ()) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;
SELECT sum(sum(abs(number))) OVER () FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;

SELECT * APPLY abs APPLY sum FROM (SELECT toNullable(number) AS n FROM numbers(3)) GROUP BY n WITH ROLLUP ORDER BY 1;
SELECT sum(abs(n)) FROM (SELECT toNullable(number) AS n FROM numbers(3)) GROUP BY n WITH ROLLUP ORDER BY 1;
SELECT * APPLY length APPLY sum FROM (SELECT toLowCardinality(toString(number)) AS s FROM numbers(3)) GROUP BY s WITH ROLLUP ORDER BY 1;
SELECT sum(length(s)) FROM (SELECT toLowCardinality(toString(number)) AS s FROM numbers(3)) GROUP BY s WITH ROLLUP ORDER BY 1;

SELECT number, * APPLY abs APPLY sum FROM numbers(3) GROUP BY ALL WITH ROLLUP ORDER BY 1, 2;
SELECT number, sum(abs(number)) FROM numbers(3) GROUP BY ALL WITH ROLLUP ORDER BY 1, 2;
SELECT * APPLY abs APPLY sum FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1 SETTINGS group_by_use_nulls = 0;
SELECT sum(abs(number)) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1 SETTINGS group_by_use_nulls = 0;
SELECT * APPLY abs APPLY sum FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;
SELECT sum(abs(number)) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;

SELECT * APPLY abs APPLY (x -> (x, sum(1))) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;
SELECT (abs(number), sum(1)) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;
WITH (x -> x) AS sum SELECT * APPLY abs APPLY sum FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;
SELECT number, * APPLY abs APPLY sum, * APPLY abs APPLY toString FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;
SELECT number, sum(abs(number)), toString(abs(number)) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;
SELECT * APPLY abs APPLY (x -> tuple(arrayMap((y -> y) AS sum, [1]), toTypeName(sum(x)))) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;
WITH (x -> x) AS sum SELECT * APPLY abs APPLY sum APPLY max FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;
WITH (x -> x) AS sum SELECT max(sum(abs(number))) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;
SELECT * APPLY (x -> x + (SELECT count() FROM numbers(2))) APPLY sum FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;
SELECT sum(number + (SELECT count() FROM numbers(2))) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;
