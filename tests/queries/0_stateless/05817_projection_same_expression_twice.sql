-- a projection can have one expression two times

DROP TABLE IF EXISTS t_projection_twice;

CREATE TABLE t_projection_twice
(
    a Int32,
    c String,
    PROJECTION p_normal (SELECT a + 1, a + 1, plus(a, 1), c WHERE a > 0 ORDER BY c),
    PROJECTION p_aggregate (SELECT a % 10, a % 10, max(a % 10), max(a % 10), count() GROUP BY a % 10)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS materialize_projections_on_insert = 1;

INSERT INTO t_projection_twice VALUES (1, 'x'), (2, 'y'), (12, 'z');

SELECT name, column
FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_twice' AND active
ORDER BY name, column_position;

SELECT a % 10 AS k, max(a % 10), count() FROM t_projection_twice GROUP BY k ORDER BY k
SETTINGS force_optimize_projection = 1, force_optimize_projection_name = 'p_aggregate';

DROP TABLE t_projection_twice;
