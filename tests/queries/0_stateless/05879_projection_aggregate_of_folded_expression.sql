-- the analyzer folds `b IS NOT NULL` to a constant: the projection still reads `b` and stores the argument as before

DROP TABLE IF EXISTS t_projection_folded_argument;

CREATE TABLE t_projection_folded_argument
(
    a UInt8,
    b UInt64,
    PROJECTION p_count (SELECT count(b IS NOT NULL)),
    PROJECTION p_sum (SELECT a, sum(b IS NOT NULL), count(toTypeName(b)), count(1 + 1) GROUP BY a),
    PROJECTION p_if (SELECT if(b IS NOT NULL, a, 0), count() GROUP BY if(b IS NOT NULL, a, 0))
)
ENGINE = MergeTree ORDER BY a
SETTINGS materialize_projections_on_insert = 1;

INSERT INTO t_projection_folded_argument VALUES (1, 10), (1, 20), (2, 30);

SELECT name, column FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_folded_argument' AND active
ORDER BY name, column_position;

-- the optimizer analyzes each projection of the table
SELECT sum(a) FROM t_projection_folded_argument SETTINGS optimize_use_implicit_projections = 0;

SELECT a, sum(b IS NOT NULL) FROM t_projection_folded_argument GROUP BY a ORDER BY a;

DROP TABLE t_projection_folded_argument;
