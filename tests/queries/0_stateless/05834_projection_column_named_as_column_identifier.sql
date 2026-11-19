-- a column of the table can have the name that the planner gives to another column

DROP TABLE IF EXISTS t_projection_identifier_name;

CREATE TABLE t_projection_identifier_name
(
    a Int64,
    `__table1.a` Int64,
    PROJECTION p_normal (SELECT a, `__table1.a` WHERE a ORDER BY a),
    PROJECTION p_aggregate (SELECT sum(a) GROUP BY `__table1.a`)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS materialize_projections_on_insert = 1;

INSERT INTO t_projection_identifier_name VALUES (1, 100), (1, 200), (0, 300);

SELECT 'p_normal', * FROM mergeTreeProjection(currentDatabase(), 't_projection_identifier_name', 'p_normal') ORDER BY ALL;

SELECT 'p_aggregate', `__table1.a`, finalizeAggregation(`sum(a)`)
FROM mergeTreeProjection(currentDatabase(), 't_projection_identifier_name', 'p_aggregate') ORDER BY ALL;

-- the queries read the projections
SELECT a, `__table1.a` FROM t_projection_identifier_name WHERE a ORDER BY ALL
SETTINGS force_optimize_projection = 1, force_optimize_projection_name = 'p_normal';

SELECT `__table1.a`, sum(a) FROM t_projection_identifier_name GROUP BY 1 ORDER BY ALL
SETTINGS force_optimize_projection = 1, force_optimize_projection_name = 'p_aggregate';

-- the projection does not have the sum of this column
SELECT `__table1.a`, sum(`__table1.a`) FROM t_projection_identifier_name GROUP BY 1 ORDER BY ALL;

-- a mutation calculates the projections for the existing part
ALTER TABLE t_projection_identifier_name ADD PROJECTION p_normal_added (SELECT a, `__table1.a` WHERE a ORDER BY `__table1.a`);
ALTER TABLE t_projection_identifier_name ADD PROJECTION p_aggregate_added (SELECT max(a) GROUP BY `__table1.a`);
ALTER TABLE t_projection_identifier_name MATERIALIZE PROJECTION p_normal_added SETTINGS mutations_sync = 2;
ALTER TABLE t_projection_identifier_name MATERIALIZE PROJECTION p_aggregate_added SETTINGS mutations_sync = 2;

SELECT 'p_normal_added', * FROM mergeTreeProjection(currentDatabase(), 't_projection_identifier_name', 'p_normal_added') ORDER BY ALL;

SELECT 'p_aggregate_added', `__table1.a`, finalizeAggregation(`max(a)`)
FROM mergeTreeProjection(currentDatabase(), 't_projection_identifier_name', 'p_aggregate_added') ORDER BY ALL;

DROP TABLE t_projection_identifier_name;
