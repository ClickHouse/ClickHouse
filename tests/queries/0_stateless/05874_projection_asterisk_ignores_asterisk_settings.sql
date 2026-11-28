-- `*` in a projection has no MATERIALIZED and no ALIAS columns, whatever the asterisk settings are

DROP TABLE IF EXISTS t_projection_asterisk_settings;

CREATE TABLE t_projection_asterisk_settings (a Int32, al ALIAS a + 1, b Int32, m MATERIALIZED a * 2)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS materialize_projections_on_insert = 1;

SET asterisk_include_materialized_columns = 1, asterisk_include_alias_columns = 1;
ALTER TABLE t_projection_asterisk_settings ADD PROJECTION p_normal (SELECT * WHERE a > 0 ORDER BY b);
ALTER TABLE t_projection_asterisk_settings ADD PROJECTION p_aggregate (SELECT *, count() GROUP BY *);

INSERT INTO t_projection_asterisk_settings (a, b) VALUES (1, 10), (2, 20), (0, 30);

SELECT name, column FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_asterisk_settings' AND active
ORDER BY name, column_position;

SELECT * FROM mergeTreeProjection(currentDatabase(), 't_projection_asterisk_settings', 'p_normal') ORDER BY ALL;

DROP TABLE t_projection_asterisk_settings;
