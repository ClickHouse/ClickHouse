-- a projection can have a column and a subcolumn of that column

DROP TABLE IF EXISTS t_projection_subcolumn;

CREATE TABLE t_projection_subcolumn
(
    a Tuple(x Int32, y Int32),
    b Int32,
    PROJECTION p_normal (SELECT a, a.x WHERE b > 0 ORDER BY b),
    PROJECTION p_aggregate (SELECT count() GROUP BY a, a.x)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS materialize_projections_on_insert = 1;

INSERT INTO t_projection_subcolumn VALUES ((1, 2), 3), ((4, 5), -6), ((1, 2), 7);

SELECT a, a.x, b FROM mergeTreeProjection(currentDatabase(), 't_projection_subcolumn', 'p_normal') ORDER BY b;
SELECT a, a.x, finalizeAggregation(`count()`) FROM mergeTreeProjection(currentDatabase(), 't_projection_subcolumn', 'p_aggregate') ORDER BY a;

ALTER TABLE t_projection_subcolumn ADD PROJECTION p_added (SELECT a, a.x WHERE b < 0 ORDER BY b);
ALTER TABLE t_projection_subcolumn MATERIALIZE PROJECTION p_added SETTINGS mutations_sync = 2;

SELECT a, a.x, b FROM mergeTreeProjection(currentDatabase(), 't_projection_subcolumn', 'p_added') ORDER BY b;

SELECT a, a.x, count() FROM t_projection_subcolumn GROUP BY a, a.x ORDER BY a;
SELECT a, a.x, count() FROM t_projection_subcolumn GROUP BY a, a.x ORDER BY a SETTINGS force_optimize_projection = 1;

DROP TABLE t_projection_subcolumn;
