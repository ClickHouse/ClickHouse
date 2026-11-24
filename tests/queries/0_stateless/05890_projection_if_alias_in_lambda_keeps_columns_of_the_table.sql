-- the expression of an alias does not go into a lambda whose parameter hides a column that the expression reads

SET optimize_if_chain_to_multiif = 1, optimize_if_transform_const_strings_to_lowcardinality = 0;

DROP TABLE IF EXISTS t_projection_if_alias_lambda;

CREATE TABLE t_projection_if_alias_lambda (x UInt8, arr Array(UInt8))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS materialize_projections_on_insert = 1;

ALTER TABLE t_projection_if_alias_lambda ADD PROJECTION p_key (WITH if(x = 1, 2, 3) AS inner SELECT count() GROUP BY arrayMap(x -> if(x = 0, 1, inner), arr));
ALTER TABLE t_projection_if_alias_lambda ADD PROJECTION p_where (WITH if(x = 1, 2, 3) AS inner SELECT x, arr WHERE arrayExists(x -> if(x = 0, 1, inner) = 2, arr) ORDER BY x);

-- the parameter hides no column here, so the chain of `if` changes
ALTER TABLE t_projection_if_alias_lambda ADD PROJECTION p_other_name (WITH if(x = 1, 2, 3) AS inner SELECT count() GROUP BY arrayMap(y -> if(y = 0, 1, inner), arr));

INSERT INTO t_projection_if_alias_lambda (x, arr) VALUES (1, [5]), (2, [5]);

SELECT name, sorting_key FROM system.projections
WHERE database = currentDatabase() AND table = 't_projection_if_alias_lambda'
ORDER BY name;

SELECT 'p_key', * EXCEPT (`count()`), finalizeAggregation(`count()`)
FROM mergeTreeProjection(currentDatabase(), 't_projection_if_alias_lambda', 'p_key') ORDER BY ALL;

SELECT 'p_where', x, arr
FROM mergeTreeProjection(currentDatabase(), 't_projection_if_alias_lambda', 'p_where') ORDER BY x;

SELECT 'p_other_name', * EXCEPT (`count()`), finalizeAggregation(`count()`)
FROM mergeTreeProjection(currentDatabase(), 't_projection_if_alias_lambda', 'p_other_name') ORDER BY ALL;

-- the same expressions over the table, without projections
WITH if(x = 1, 2, 3) AS inner SELECT 'table', arrayMap(x -> if(x = 0, 1, inner), arr) AS k, count()
FROM t_projection_if_alias_lambda GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 0;

WITH if(x = 1, 2, 3) AS inner SELECT 'table', x, arr
FROM t_projection_if_alias_lambda WHERE arrayExists(x -> if(x = 0, 1, inner) = 2, arr) ORDER BY x
SETTINGS optimize_use_projections = 0;

DROP TABLE t_projection_if_alias_lambda;
