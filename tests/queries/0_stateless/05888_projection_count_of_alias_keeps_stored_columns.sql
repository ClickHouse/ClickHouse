-- `count(1)` and `sum(1)` are stored as `count()` only where the query writes the literal: an alias of a literal and a changed `if` keep their columns

SET optimize_normalize_count_variants = 1;

DROP TABLE IF EXISTS t_projection_count_alias;

CREATE TABLE t_projection_count_alias (a UInt8, b UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS materialize_projections_on_insert = 1;

ALTER TABLE t_projection_count_alias ADD PROJECTION p_literal (SELECT a, count(1), sum(1) GROUP BY a);
ALTER TABLE t_projection_count_alias ADD PROJECTION p_alias (WITH 1 AS one SELECT a, count(one), sum(one) GROUP BY a);
ALTER TABLE t_projection_count_alias ADD PROJECTION p_select_alias (SELECT 1 AS one, a, count(one) GROUP BY a);
ALTER TABLE t_projection_count_alias ADD PROJECTION p_alias_of_alias (WITH 1 AS one, one AS uno SELECT a, sum(uno) GROUP BY a);
ALTER TABLE t_projection_count_alias ADD PROJECTION p_both (WITH 1 AS one SELECT a, count(1), count(one), sum(1), sum(one) GROUP BY a);
ALTER TABLE t_projection_count_alias ADD PROJECTION p_in_with (WITH count(1) AS n SELECT a, n GROUP BY a);
ALTER TABLE t_projection_count_alias ADD PROJECTION p_if (SELECT a, count(if(1, 1, a)), sum(if(1, 1, a)) GROUP BY a);
ALTER TABLE t_projection_count_alias ADD PROJECTION p_if_alias (WITH 1 AS c SELECT a, count(if(c, 1, a)), sum(if(c, 1, a)) GROUP BY a);
ALTER TABLE t_projection_count_alias ADD PROJECTION p_if_else (WITH 0 AS c SELECT a, sum(if(c, a, 1)), max(if(c, a, 2)) GROUP BY a);
ALTER TABLE t_projection_count_alias ADD PROJECTION p_if_key (WITH 1 AS c SELECT if(c, 5, a), a, count() GROUP BY if(c, 5, a), a);

INSERT INTO t_projection_count_alias VALUES (1, 10), (1, 10), (2, 20);

SELECT name, column, type FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_count_alias' AND active
ORDER BY name, column_position;

SELECT a, finalizeAggregation(`count(1)`), finalizeAggregation(`sum(1)`)
FROM mergeTreeProjection(currentDatabase(), 't_projection_count_alias', 'p_alias') ORDER BY a;

DROP TABLE t_projection_count_alias;
