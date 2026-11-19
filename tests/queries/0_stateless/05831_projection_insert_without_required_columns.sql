-- the query of the projection reads no column of the table, and an INSERT can calculate the projection

DROP TABLE IF EXISTS t_projection_no_columns;

CREATE TABLE t_projection_no_columns
(
    s String,
    a UInt8 ALIAS 1,
    PROJECTION p (SELECT a, count() GROUP BY a)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS materialize_projections_on_insert = 1;

INSERT INTO t_projection_no_columns VALUES ('x'), ('y');

SELECT name, column
FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_no_columns' AND active
ORDER BY name, column_position;

SELECT finalizeAggregation(`count()`) FROM mergeTreeProjection(currentDatabase(), 't_projection_no_columns', 'p');

SELECT a, count() FROM t_projection_no_columns GROUP BY a;

DROP TABLE t_projection_no_columns;
