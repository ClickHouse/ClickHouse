-- a GROUP BY key that the analyzer folds to a constant is not a key column of the projection, and the column that it reads stays readable

DROP TABLE IF EXISTS t_projection_constant_key;

CREATE TABLE t_projection_constant_key
(
    a UInt8,
    b UInt64,
    PROJECTION p_extra_key (SELECT a, b, count() GROUP BY a, b IS NOT NULL, b),
    PROJECTION p_only_in_key (SELECT a, count() GROUP BY a, b IS NOT NULL)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS materialize_projections_on_insert = 1;

INSERT INTO t_projection_constant_key VALUES (1, 10), (1, 10), (2, 20);

SELECT name, sorting_key FROM system.projections
WHERE database = currentDatabase() AND table = 't_projection_constant_key'
ORDER BY name;

SELECT name, column FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_constant_key' AND active
ORDER BY name, column_position;

SELECT a, b, count() FROM t_projection_constant_key WHERE a = 1 AND b = 10 GROUP BY a, b;

DROP TABLE t_projection_constant_key;
