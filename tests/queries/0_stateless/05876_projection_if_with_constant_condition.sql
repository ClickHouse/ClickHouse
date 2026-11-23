-- a query reads a projection whose key is an `if` with a constant condition

DROP TABLE IF EXISTS t_projection_if_constant;

CREATE TABLE t_projection_if_constant (a Int32, b Int32, PROJECTION p (SELECT if(1, a, b), count() GROUP BY if(1, a, b)))
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_projection_if_constant VALUES (1, 10), (1, 20), (2, 30);

SELECT column FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_if_constant' AND active
ORDER BY column_position;

SELECT if(1, a, b) AS k, count() FROM t_projection_if_constant GROUP BY k ORDER BY k
SETTINGS force_optimize_projection = 1;

DROP TABLE t_projection_if_constant;
