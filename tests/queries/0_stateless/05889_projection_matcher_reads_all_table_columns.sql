-- a matcher of a projection sees all the columns of the table when the projection is calculated, not only the columns that the projection reads

SET optimize_read_in_order = 0;

DROP TABLE IF EXISTS t_projection_matcher;

CREATE TABLE t_projection_matcher (a UInt8, b UInt8)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS materialize_projections_on_insert = 1;

ALTER TABLE t_projection_matcher ADD PROJECTION p_except_strict (SELECT count() GROUP BY * EXCEPT STRICT (b));
ALTER TABLE t_projection_matcher ADD PROJECTION p_replace_strict (SELECT * REPLACE STRICT (a + 1 AS b), max(a) GROUP BY * REPLACE STRICT (a + 1 AS b));
ALTER TABLE t_projection_matcher ADD PROJECTION p_replace (SELECT * REPLACE (a + 2 AS b), min(a) GROUP BY * REPLACE (a + 2 AS b));
ALTER TABLE t_projection_matcher ADD PROJECTION p_apply (SELECT * EXCEPT STRICT (b) APPLY (toString), sum(a) GROUP BY * EXCEPT STRICT (b) APPLY (toString));
ALTER TABLE t_projection_matcher ADD PROJECTION p_normal (SELECT * REPLACE (a + 3 AS b) WHERE a > 0 ORDER BY a);

INSERT INTO t_projection_matcher SELECT number % 3, number % 5 FROM numbers(30);

SELECT name, column, type FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_matcher' AND active
ORDER BY name, column_position;

-- the table is loaded again, and a merge calculates the projections again
DETACH TABLE t_projection_matcher;
ATTACH TABLE t_projection_matcher;
INSERT INTO t_projection_matcher SELECT number % 3, number % 5 FROM numbers(30);
OPTIMIZE TABLE t_projection_matcher FINAL;

-- the optimizer analyzes each projection for this query
SELECT a, count() FROM t_projection_matcher GROUP BY a ORDER BY a
SETTINGS force_optimize_projection = 1;

SELECT a, `plus(a, 1)`, finalizeAggregation(`max(a)`) FROM mergeTreeProjection(currentDatabase(), 't_projection_matcher', 'p_replace_strict') ORDER BY a;
SELECT a, `plus(a, 2)`, finalizeAggregation(`min(a)`) FROM mergeTreeProjection(currentDatabase(), 't_projection_matcher', 'p_replace') ORDER BY a;
SELECT `toString(a)`, finalizeAggregation(`sum(a)`) FROM mergeTreeProjection(currentDatabase(), 't_projection_matcher', 'p_apply') ORDER BY 1;
SELECT a, `plus(a, 3)`, count() FROM mergeTreeProjection(currentDatabase(), 't_projection_matcher', 'p_normal') GROUP BY ALL ORDER BY a;

DROP TABLE t_projection_matcher;
