-- an INSERT with this setting must not change the aggregate function that the projection stores

DROP TABLE IF EXISTS t_projection_count_distinct_if;
CREATE TABLE t_projection_count_distinct_if (k UInt8, s String, c UInt8, PROJECTION p (SELECT countDistinctIf(s, c) GROUP BY k))
ENGINE = MergeTree ORDER BY k
SETTINGS materialize_projections_on_insert = 1;

SET rewrite_count_distinct_if_with_count_distinct_implementation = 1;
INSERT INTO t_projection_count_distinct_if VALUES (1, 'a', 1), (1, 'a', 1), (1, 'b', 1), (1, 'c', 0), (2, 'e', 1);
SET rewrite_count_distinct_if_with_count_distinct_implementation = 0;

SELECT k, countDistinctIf(s, c) FROM t_projection_count_distinct_if GROUP BY k ORDER BY k SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE t_projection_count_distinct_if;
