-- a normal projection with WHERE stores the columns that a window function reads, not its result

DROP TABLE IF EXISTS t_projection_window;

CREATE TABLE t_projection_window
(
    a Int32,
    b Int32,
    c String,
    PROJECTION p_window (SELECT a, sum(b) OVER () WHERE a > 0 ORDER BY a),
    PROJECTION p_mixed (SELECT a + 1, upper(c), sum(b) OVER (PARTITION BY c ORDER BY a) WHERE a > 0 ORDER BY a)
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_projection_window VALUES (0, 1, 'x'), (1, 2, 'x'), (2, 5, 'y'), (3, 7, 'y');

SELECT name, column
FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_window' AND active
ORDER BY name, column;

SELECT name, rows
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_projection_window' AND active
ORDER BY name;

SELECT a, sum(b) OVER () FROM t_projection_window WHERE a > 0 ORDER BY a SETTINGS force_optimize_projection = 1;

SELECT a + 1, upper(c), sum(b) OVER (PARTITION BY c ORDER BY a) FROM t_projection_window WHERE a > 0 ORDER BY a SETTINGS force_optimize_projection = 1;

DROP TABLE t_projection_window;
