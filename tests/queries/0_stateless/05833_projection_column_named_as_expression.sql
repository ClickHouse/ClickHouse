-- a column of the table can have the name that the planner gives to an expression of the projection

-- parallel replicas can reject a forced projection, and they do not read a column that has such a name
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_projection_name_collision;

CREATE TABLE t_projection_name_collision
(
    a Int32,
    `modulo(a, 2_UInt8)` Int64,
    PROJECTION p_normal (SELECT a % 2, `modulo(a, 2_UInt8)` WHERE a > 0 ORDER BY a),
    PROJECTION p_keys (SELECT count() GROUP BY a % 2, `modulo(a, 2_UInt8)`),
    PROJECTION p_argument (SELECT sum(a % 2) GROUP BY `modulo(a, 2_UInt8)`)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS materialize_projections_on_insert = 1;

INSERT INTO t_projection_name_collision VALUES (1, 100), (1, 200), (4, 100);

SELECT 'p_normal', * FROM mergeTreeProjection(currentDatabase(), 't_projection_name_collision', 'p_normal') ORDER BY ALL;

SELECT 'p_keys', `modulo(a, 2)`, `modulo(a, 2_UInt8)`, finalizeAggregation(`count()`)
FROM mergeTreeProjection(currentDatabase(), 't_projection_name_collision', 'p_keys') ORDER BY ALL;

SELECT 'p_argument', `modulo(a, 2_UInt8)`, finalizeAggregation(`sum(modulo(a, 2))`)
FROM mergeTreeProjection(currentDatabase(), 't_projection_name_collision', 'p_argument') ORDER BY ALL;

-- the queries read the projections
SELECT a, `modulo(a, 2_UInt8)` FROM t_projection_name_collision WHERE a > 0 ORDER BY ALL
SETTINGS force_optimize_projection = 1, force_optimize_projection_name = 'p_normal';

SELECT a % 2, `modulo(a, 2_UInt8)`, count() FROM t_projection_name_collision GROUP BY ALL ORDER BY ALL
SETTINGS force_optimize_projection = 1, force_optimize_projection_name = 'p_keys';

SELECT `modulo(a, 2_UInt8)`, sum(a % 2) FROM t_projection_name_collision GROUP BY 1 ORDER BY ALL
SETTINGS force_optimize_projection = 1, force_optimize_projection_name = 'p_argument';

-- a mutation calculates the projection for the existing part
ALTER TABLE t_projection_name_collision ADD PROJECTION p_added (SELECT a % 2, `modulo(a, 2_UInt8)` WHERE a < 5 ORDER BY a);
ALTER TABLE t_projection_name_collision MATERIALIZE PROJECTION p_added SETTINGS mutations_sync = 2;

SELECT 'p_added', * FROM mergeTreeProjection(currentDatabase(), 't_projection_name_collision', 'p_added') ORDER BY ALL;

DROP TABLE t_projection_name_collision;
