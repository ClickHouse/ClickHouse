-- a lambda of a projection can have a parameter with the name of a column that an ALIAS column or an alias reads

DROP TABLE IF EXISTS t_projection_lambda_alias;

CREATE TABLE t_projection_lambda_alias
(
    x Int32,
    arr Array(Int32),
    y Int32 ALIAS x,
    PROJECTION p_alias_column (SELECT count() GROUP BY arrayMap(x -> x - y, arr)),
    PROJECTION p_alias (SELECT x AS z, arrayMap(x -> x - z, arr) AS k, count() GROUP BY z, k),
    PROJECTION p_normal (SELECT x, arr WHERE arrayExists(x -> x > y, arr) ORDER BY x)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS materialize_projections_on_insert = 1;

INSERT INTO t_projection_lambda_alias (x, arr) VALUES (10, [11]), (20, [11]);

SELECT 'table', arrayMap(x -> x - y, arr) AS k, count() FROM t_projection_lambda_alias GROUP BY k ORDER BY k SETTINGS optimize_use_projections = 0;
SELECT 'p_alias_column', * EXCEPT (`count()`), finalizeAggregation(`count()`) FROM mergeTreeProjection(currentDatabase(), 't_projection_lambda_alias', 'p_alias_column') ORDER BY ALL;
SELECT 'p_alias', * EXCEPT (`count()`), finalizeAggregation(`count()`) FROM mergeTreeProjection(currentDatabase(), 't_projection_lambda_alias', 'p_alias') ORDER BY ALL;

SELECT 'table', x FROM t_projection_lambda_alias WHERE arrayExists(x -> x > y, arr) SETTINGS optimize_use_projections = 0;
SELECT 'p_normal', x FROM mergeTreeProjection(currentDatabase(), 't_projection_lambda_alias', 'p_normal');

DROP TABLE t_projection_lambda_alias;
