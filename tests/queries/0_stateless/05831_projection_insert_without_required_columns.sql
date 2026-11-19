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

-- the smallest column of the table is EPHEMERAL, and a part does not store it
DROP TABLE IF EXISTS t_projection_ephemeral;

CREATE TABLE t_projection_ephemeral
(
    s String,
    e UInt8 EPHEMERAL,
    PROJECTION p (SELECT count())
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS materialize_projections_on_insert = 1;

INSERT INTO t_projection_ephemeral (s) VALUES ('x'), ('y'), ('z');

SELECT finalizeAggregation(`count()`) FROM mergeTreeProjection(currentDatabase(), 't_projection_ephemeral', 'p');

SELECT count() FROM t_projection_ephemeral SETTINGS optimize_trivial_count_query = 0;

DROP TABLE t_projection_ephemeral;
