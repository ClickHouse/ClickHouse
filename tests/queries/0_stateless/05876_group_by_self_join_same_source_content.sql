-- The same table is joined with itself, and the two table expressions have no aliases,
-- so they are equal by content. `COLUMNS('^x$')` expands to `x` of the left table and `x` of the right table.
-- The expression `left.x - right.x` must not be considered equal to the GROUP BY key `left.x - left.x`.

DROP TABLE IF EXISTS t_self_join;
CREATE TABLE t_self_join (x UInt64, y UInt64) ENGINE = Memory;
INSERT INTO t_self_join VALUES (1, 10), (2, 20);

SELECT minus(COLUMNS('^x$')) AS d
FROM t_self_join JOIN {CLICKHOUSE_DATABASE:Identifier}.t_self_join ON 1
ORDER BY d;

SELECT minus(COLUMNS('^x$')) AS d
FROM t_self_join JOIN {CLICKHOUSE_DATABASE:Identifier}.t_self_join ON 1
GROUP BY t_self_join.x - t_self_join.x
ORDER BY d; -- { serverError NOT_AN_AGGREGATE }

-- Both columns are from the left table, so the expression is equal to the key.
SELECT t_self_join.x - t_self_join.x AS d
FROM t_self_join JOIN {CLICKHOUSE_DATABASE:Identifier}.t_self_join ON 1
GROUP BY t_self_join.x - t_self_join.x
ORDER BY d;

DROP TABLE t_self_join;
