-- a tuple is one key column of a projection, also when GROUP BY refers to it by an alias or by a position

DROP TABLE IF EXISTS t_projection_tuple_key;

CREATE TABLE t_projection_tuple_key (a Int32, b Int32)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS materialize_projections_on_insert = 1;

ALTER TABLE t_projection_tuple_key ADD PROJECTION p_alias (SELECT tuple(a, b) AS k, count() GROUP BY k);
ALTER TABLE t_projection_tuple_key ADD PROJECTION p_alias_of_alias (SELECT (a, b) AS k, k AS k2, sum(a) GROUP BY k2);
ALTER TABLE t_projection_tuple_key ADD PROJECTION p_two_keys (SELECT tuple(a, b) AS k, a AS x, max(b) GROUP BY k, x);
SET enable_positional_arguments_for_projections = 1;
ALTER TABLE t_projection_tuple_key ADD PROJECTION p_position (SELECT *, tuple(a, b), min(a) GROUP BY 1, 2, 3);
SET enable_positional_arguments_for_projections = 0;

INSERT INTO t_projection_tuple_key VALUES (1, 10), (1, 10), (2, 20);

SELECT name, sorting_key FROM system.projections
WHERE database = currentDatabase() AND table = 't_projection_tuple_key'
ORDER BY name;

SELECT name, column, type FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_tuple_key' AND active
ORDER BY name, column_position;

SELECT `tuple(a, b)`, finalizeAggregation(`count()`) FROM mergeTreeProjection(currentDatabase(), 't_projection_tuple_key', 'p_alias') ORDER BY ALL;

SELECT tuple(a, b) AS k, count() FROM t_projection_tuple_key GROUP BY k ORDER BY k;

DROP TABLE t_projection_tuple_key;
