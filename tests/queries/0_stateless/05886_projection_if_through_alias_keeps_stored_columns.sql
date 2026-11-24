-- an argument of `if` can be an alias of the query: `if` is changed as if the expression of the alias is in its place, so the stored columns stay as they were

-- with this setting a chain of `if` stays as it is
SET optimize_if_transform_const_strings_to_lowcardinality = 0;

DROP TABLE IF EXISTS t_projection_if_alias;

CREATE TABLE t_projection_if_alias (a UInt8, b UInt64, one UInt8 ALIAS 1)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS materialize_projections_on_insert = 1;

ALTER TABLE t_projection_if_alias ADD PROJECTION p_with (WITH 1 AS c SELECT if(c, a, b), count() GROUP BY if(c, a, b));
ALTER TABLE t_projection_if_alias ADD PROJECTION p_select (SELECT if(c, a, b), count(), 1 AS c GROUP BY if(c, a, b));
ALTER TABLE t_projection_if_alias ADD PROJECTION p_null (WITH NULL AS c SELECT if(c, a, b), count() GROUP BY if(c, a, b));
ALTER TABLE t_projection_if_alias ADD PROJECTION p_cast (WITH CAST(1, 'UInt8') AS c SELECT if(c, a, b), count() GROUP BY if(c, a, b));
ALTER TABLE t_projection_if_alias ADD PROJECTION p_alias_of_alias (WITH 1 AS c, c AS d SELECT if(d, a, b), count() GROUP BY if(d, a, b));
ALTER TABLE t_projection_if_alias ADD PROJECTION p_argument (WITH 1 AS c SELECT a, sum(if(c, a, b)) GROUP BY a);
ALTER TABLE t_projection_if_alias ADD PROJECTION p_nested (WITH 1 AS c SELECT if(a > 1, if(c, a, b), 0), count() GROUP BY if(a > 1, if(c, a, b), 0));
ALTER TABLE t_projection_if_alias ADD PROJECTION p_key_alias (WITH 1 AS c SELECT if(c, a, b) AS k, count() GROUP BY k);
ALTER TABLE t_projection_if_alias ADD PROJECTION p_alias_as_column (WITH 1 AS a SELECT if(a, b, 7), count() GROUP BY if(a, b, 7));
ALTER TABLE t_projection_if_alias ADD PROJECTION p_if_in_with (WITH 1 AS c, if(c, a, b) AS x SELECT x, count() GROUP BY x);
ALTER TABLE t_projection_if_alias ADD PROJECTION p_normal (WITH 1 AS c SELECT a, if(c, a, b) AS x WHERE b > 10 ORDER BY a);

-- these stay `if`: a parameter of a lambda and an `ALIAS` column are not aliases of the query
ALTER TABLE t_projection_if_alias ADD PROJECTION p_lambda (WITH 1 AS c SELECT arrayMap(c -> if(c, 1, 2), [a]), count() GROUP BY arrayMap(c -> if(c, 1, 2), [a]));
ALTER TABLE t_projection_if_alias ADD PROJECTION p_alias_column (SELECT if(one, a, b), count() GROUP BY if(one, a, b));

SET optimize_if_chain_to_multiif = 1;
ALTER TABLE t_projection_if_alias ADD PROJECTION p_chain (WITH if(a = 1, 2, 3) AS inner SELECT if(a = 0, 1, inner), count() GROUP BY if(a = 0, 1, inner));
SET optimize_if_chain_to_multiif = 0;

INSERT INTO t_projection_if_alias VALUES (1, 10), (1, 10), (2, 20);

SELECT name, sorting_key FROM system.projections
WHERE database = currentDatabase() AND table = 't_projection_if_alias'
ORDER BY name;

SELECT name, column, type FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_if_alias' AND active
ORDER BY name, column_position;

SELECT a, count() FROM t_projection_if_alias GROUP BY a ORDER BY a
SETTINGS force_optimize_projection = 1, force_optimize_projection_name = 'p_with';

DROP TABLE t_projection_if_alias;
