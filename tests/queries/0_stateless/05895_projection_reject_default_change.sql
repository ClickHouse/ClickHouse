-- A part that does not store a column computes it from its default expression, and so does a projection built for the part:
-- a change of the expression of a column that a projection computes from is rejected, as the projection would keep the old values.

DROP TABLE IF EXISTS t_projection_default;

CREATE TABLE t_projection_default (id UInt64, dt DateTime('UTC'))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
INSERT INTO t_projection_default SELECT number, toDateTime('2026-01-01 00:00:00', 'UTC') + INTERVAL number HOUR FROM numbers(5);
ALTER TABLE t_projection_default ADD COLUMN d DateTime('UTC') DEFAULT dt;
ALTER TABLE t_projection_default ADD COLUMN h UInt8 DEFAULT toHour(d);
ALTER TABLE t_projection_default ADD COLUMN m UInt8 MATERIALIZED toMinute(dt);
ALTER TABLE t_projection_default ADD COLUMN other UInt8 DEFAULT 1;
ALTER TABLE t_projection_default ADD PROJECTION p (SELECT h, m, count() GROUP BY h, m);
ALTER TABLE t_projection_default MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2;

SELECT '--- the default of a column the projection computes from';
ALTER TABLE t_projection_default MODIFY COLUMN h UInt8 DEFAULT toHour(d) + 10; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_projection_default MODIFY COLUMN h REMOVE DEFAULT; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_projection_default MODIFY COLUMN m UInt8 MATERIALIZED toMinute(dt) + 1; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

SELECT '--- the default of a column that the default of such a column reads';
ALTER TABLE t_projection_default MODIFY COLUMN d DateTime('UTC') DEFAULT dt + INTERVAL 10 HOUR; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

SELECT '--- the same results with and without the projection';
SELECT groupArray(h) FROM (SELECT h, count() FROM t_projection_default GROUP BY h ORDER BY h) SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT groupArray(h) FROM (SELECT h, count() FROM t_projection_default GROUP BY h ORDER BY h) SETTINGS optimize_use_projections = 0;

SELECT '--- other changes are allowed';
ALTER TABLE t_projection_default MODIFY COLUMN other UInt8 DEFAULT 2;
ALTER TABLE t_projection_default MODIFY COLUMN h COMMENT 'hour';
ALTER TABLE t_projection_default MODIFY COLUMN h UInt8 DEFAULT toHour(d);
-- A rename renames the column in the expressions that read it, which keeps the values.
ALTER TABLE t_projection_default RENAME COLUMN d TO d2;
ALTER TABLE t_projection_default RENAME COLUMN dt TO dt2;
SELECT name, default_expression FROM system.columns WHERE database = currentDatabase() AND table = 't_projection_default' AND name IN ('d2', 'h') ORDER BY name;
ALTER TABLE t_projection_default DROP PROJECTION p, MODIFY COLUMN h UInt8 DEFAULT toHour(d2) + 10;
SELECT groupArray(h) FROM (SELECT h FROM t_projection_default ORDER BY h);

DROP TABLE t_projection_default;
