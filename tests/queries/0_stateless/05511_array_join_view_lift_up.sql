-- https://github.com/ClickHouse/ClickHouse/issues/123842

DROP VIEW IF EXISTS v2_05511;
DROP VIEW IF EXISTS v1_05511;
DROP TABLE IF EXISTS t_05511;

CREATE TABLE t_05511 (y Int64) ENGINE = Memory;
INSERT INTO t_05511 VALUES (1), (-7);

CREATE VIEW v1_05511 AS SELECT y FROM t_05511 ARRAY JOIN [1] AS a;
CREATE VIEW v2_05511 AS SELECT y FROM v1_05511 ARRAY JOIN [1] AS a;

SELECT countIf(y > 0) FROM v1_05511 ARRAY JOIN [1] AS a;
SELECT countIf(y > 0) FROM v2_05511;
SELECT y + 1 FROM v1_05511 ARRAY JOIN [1] AS a ORDER BY 1;
SELECT y + 1, a FROM v1_05511 ARRAY JOIN ['x'] AS a ORDER BY 1;
SELECT y + 1 FROM v1_05511 ARRAY JOIN [1] AS a GROUP BY y + 1 ORDER BY 1;

-- `ARRAY JOIN` over a view that already has two levels of its own: three generated names would collide.
SELECT countIf(y > 0) FROM v2_05511 ARRAY JOIN [1] AS a;
-- `LEFT ARRAY JOIN` is lifted up the same way.
SELECT y + 1 FROM v1_05511 LEFT ARRAY JOIN [1] AS a ORDER BY 1;
-- Several `ARRAY JOIN` expressions in the outer query, one of them colliding with the view's.
SELECT y + 1 FROM v1_05511 ARRAY JOIN [1, 2] AS a, [3, 4] AS b ORDER BY 1;
-- A `Map` argument, the other half of "array or map".
SELECT y + 1 FROM v1_05511 ARRAY JOIN map('k', 1) AS a ORDER BY 1;

DROP VIEW v2_05511;
DROP VIEW v1_05511;
DROP TABLE t_05511;
