-- a projection stores `count(1)` and `sum(1)` as `count()`, and the settings of an INSERT do not change that

DROP TABLE IF EXISTS t_projection_count_variants;

CREATE TABLE t_projection_count_variants (a Int32, PROJECTION p (SELECT a, count(1), sum(1) GROUP BY a))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS materialize_projections_on_insert = 1;

SYSTEM STOP MERGES t_projection_count_variants;

INSERT INTO t_projection_count_variants SETTINGS optimize_normalize_count_variants = 0 VALUES (1), (1), (2);
INSERT INTO t_projection_count_variants SETTINGS optimize_normalize_count_variants = 1 VALUES (1), (3);

SELECT DISTINCT column FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_count_variants' AND active
ORDER BY column;

SELECT a, finalizeAggregation(`count()`) FROM mergeTreeProjection(currentDatabase(), 't_projection_count_variants', 'p') ORDER BY ALL;

SELECT a, count() FROM t_projection_count_variants GROUP BY a ORDER BY a SETTINGS optimize_use_projections = 0;

DROP TABLE t_projection_count_variants;
