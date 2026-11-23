-- a projection that is added with `rewrite_count_distinct_if_with_count_distinct_implementation` keeps the rewrite,
-- and an INSERT without the setting does not change the name of the column

DROP TABLE IF EXISTS t_projection_count_distinct_if_rewrite;

CREATE TABLE t_projection_count_distinct_if_rewrite (a Int32, c String)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS materialize_projections_on_insert = 1;

SET rewrite_count_distinct_if_with_count_distinct_implementation = 1;
ALTER TABLE t_projection_count_distinct_if_rewrite ADD PROJECTION p (SELECT c, countDistinctIf(a, a > 1) GROUP BY c);
SET rewrite_count_distinct_if_with_count_distinct_implementation = 0;

INSERT INTO t_projection_count_distinct_if_rewrite VALUES (1, 'x'), (2, 'y'), (7, 'x'), (7, 'x');

SELECT column FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_count_distinct_if_rewrite' AND active
ORDER BY column_position;

SELECT c, finalizeAggregation(`uniqExactIf(a, greater(a, 1))`)
FROM mergeTreeProjection(currentDatabase(), 't_projection_count_distinct_if_rewrite', 'p')
ORDER BY c;

DROP TABLE t_projection_count_distinct_if_rewrite;
