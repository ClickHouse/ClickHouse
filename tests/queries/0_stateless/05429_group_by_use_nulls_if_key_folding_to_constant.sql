-- Tags: shard
-- ROLLUP, CUBE and GROUPING SETS keys `if` / `multiIf` with a constant condition the analyzer does not fold and a constant
-- chosen branch, with `group_by_use_nulls`.

SET group_by_use_nulls = 1;

SELECT 1048576 AS a, arrayMap(x -> intDiv(a, x), [1, 2, 3]) AS m FROM system.one GROUP BY if(toLowCardinality(7), a, toString(dummy) >= 'x') WITH CUBE ORDER BY ALL;
SELECT 1048576 AS a, arrayMap(x -> intDiv(a, x), [1, 2, 3]) AS m FROM system.one GROUP BY if(toLowCardinality(7), a, toString(dummy) >= 'x') WITH ROLLUP ORDER BY ALL;
SELECT 1048576 AS a, arrayMap(x -> intDiv(a, x), [1, 2, 3]) AS m FROM system.one GROUP BY GROUPING SETS ((if(toLowCardinality(7), a, toString(dummy) >= 'x')), ()) ORDER BY ALL;
SELECT 1048576 AS a, arrayMap(x -> intDiv(a, x), [1, 2, 3]) AS m FROM system.one GROUP BY multiIf(1, a, toString(dummy) >= 'x') WITH CUBE ORDER BY ALL SETTINGS optimize_multiif_to_if = 0;
SELECT 1048576 AS a, arrayMap(x -> intDiv(a, x), [1, 2, 3]) AS m FROM system.one GROUP BY multiIf(1, a, toString(dummy) >= 'x') WITH CUBE ORDER BY ALL SETTINGS optimize_multiif_to_if = 1;
SELECT 1048576 AS a, arrayMap(x -> intDiv(a, x), [1, 2, 3]) AS m FROM system.one GROUP BY if(toNullable(1), a, toString(dummy) >= 'x') WITH CUBE ORDER BY ALL;
SELECT 'abc' AS a, arrayMap(x -> concat(a, x), ['1']) AS m FROM system.one GROUP BY if(toLowCardinality(1), a, toString(dummy)) WITH CUBE ORDER BY ALL;
SELECT count() + 1048576 AS s, toTypeName(s) FROM system.one GROUP BY if(toLowCardinality(7), 1048576, toString(dummy) >= 'x') WITH CUBE ORDER BY ALL;
SELECT 1048576 AS a, leadInFrame(a) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS w, toTypeName(w) FROM system.one GROUP BY if(toLowCardinality(7), a, toString(dummy) >= 'x') WITH CUBE ORDER BY ALL;
SELECT if(toLowCardinality(7), 1048576, toString(dummy) >= 'x') AS k, count() FROM system.one GROUP BY k WITH CUBE ORDER BY ALL;
SELECT 1048576 AS a, arrayMap(x -> intDiv(a, x), [1, 2, 3]) AS m FROM remote('127.0.0.{1,2}', system.one) GROUP BY if(toLowCardinality(7), a, toString(dummy) >= 'x') WITH CUBE ORDER BY ALL;
