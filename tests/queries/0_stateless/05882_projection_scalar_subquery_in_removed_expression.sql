-- a scalar subquery keeps its value in the projection, also when the analysis removes the expression that holds it

DROP TABLE IF EXISTS t_projection_scalar_removed;
DROP TABLE IF EXISTS t_projection_scalar_removed_source;

CREATE TABLE t_projection_scalar_removed_source (v UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_projection_scalar_removed_source VALUES (10);

CREATE TABLE t_projection_scalar_removed
(
    a UInt8,
    PROJECTION p_count (SELECT count((SELECT max(v) FROM t_projection_scalar_removed_source))),
    PROJECTION p_key (SELECT count() GROUP BY (SELECT max(v) FROM t_projection_scalar_removed_source), a)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS materialize_projections_on_insert = 1;

INSERT INTO t_projection_scalar_removed VALUES (1);

-- the projections do not read the table of the subquery again
DROP TABLE t_projection_scalar_removed_source;
INSERT INTO t_projection_scalar_removed VALUES (2);

SELECT DISTINCT name, column FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_scalar_removed' AND active
ORDER BY name, column;

SELECT count() FROM t_projection_scalar_removed;

DROP TABLE t_projection_scalar_removed;
