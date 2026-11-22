-- the settings of an INSERT query do not change the data that a projection stores

DROP TABLE IF EXISTS t_projection_insert_settings;

CREATE TABLE t_projection_insert_settings
(
    x Int32,
    y Int32,
    n Nullable(Int32),
    PROJECTION p_alias (SELECT x AS y, count() GROUP BY y),
    PROJECTION p_alias_where (SELECT 100 - x AS x, y WHERE x > 50 ORDER BY x),
    PROJECTION p_null (SELECT n IN (1, NULL) AS k, sum(n), count() GROUP BY k),
    PROJECTION p_filter (SELECT x, y WHERE x > 0 ORDER BY y)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS materialize_projections_on_insert = 1;

SYSTEM STOP MERGES t_projection_insert_settings;

INSERT INTO t_projection_insert_settings VALUES (1, 10, 1), (1, 20, NULL), (2, 10, 3), (60, 3, NULL);

INSERT INTO t_projection_insert_settings
SETTINGS
    prefer_column_name_to_alias = 1,
    aggregate_functions_null_for_empty = 1,
    transform_null_in = 1,
    additional_table_filters = {'dummy.dummy': 'x > 100', 'dummy': 'x > 100', 't_projection_insert_settings': 'x > 100'}
VALUES (1, 10, 1), (1, 20, NULL), (2, 10, 3), (60, 3, NULL);

SELECT name, column
FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_insert_settings' AND active
GROUP BY name, column
ORDER BY name, min(column_position);

-- the part of each INSERT has the same rows
SELECT 'p_alias', _part, x, finalizeAggregation(`count()`)
FROM mergeTreeProjection(currentDatabase(), 't_projection_insert_settings', 'p_alias') ORDER BY ALL;

SELECT 'p_alias_where', _part, * FROM mergeTreeProjection(currentDatabase(), 't_projection_insert_settings', 'p_alias_where') ORDER BY ALL;

SELECT 'p_null', _part, `in(n, (1, NULL))`, finalizeAggregation(`sum(n)`), finalizeAggregation(`count()`)
FROM mergeTreeProjection(currentDatabase(), 't_projection_insert_settings', 'p_null') ORDER BY ALL;

SELECT 'p_filter', _part, * FROM mergeTreeProjection(currentDatabase(), 't_projection_insert_settings', 'p_filter') ORDER BY ALL;

DROP TABLE t_projection_insert_settings;
